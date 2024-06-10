package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var cmdIncremental = &cobra.Command{
	Use:   "incremental [flags]",
	Short: "Perform an incremental backup based on given list of changed files",
	Long: `
The "incremental" command takes in files that have been changed and detected by external tools (e.g. auditd), makes a copy of the latest snapshot
and modifies it during the copy to reflect the changes to the list of files provided to it, without walking the entire tree to scan for changes.

If no files are provided via the *include* arguments, the command will exit early and make no changes.

Please note, no changes are made to the existing snapshot, only to the copy that is created during this commands runtime. If anything goes wrong,
no backed-up data will be lose or damaged.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runIncremental(cmd.Context(), incrementalOptions, globalOptions)
	},
}

// IncrementalOptions collects all options for the incremental command.
type IncrementalOptions struct {
	Metadata snapshotMetadataArgs
	restic.SnapshotFilter
	includePatternOptions
}

func init() {
	cmdRoot.AddCommand(cmdIncremental)

	f := cmdIncremental.Flags()

	initMultiSnapshotFilter(f, &incrementalOptions.SnapshotFilter, true)
	initIncludePatternOptions(f, &incrementalOptions.includePatternOptions)
}

var incrementalOptions IncrementalOptions

func addNode(path string, tb *restic.TreeJSONBuilder, tree *restic.Tree) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	newNode, err := restic.NodeFromFileInfo(path, fileInfo, true)
	if err != nil {
		return err
	}
	err = tree.Insert(newNode)
	if err != nil {
		return err
	}
	err = tb.AddNode(newNode)
	if err != nil {
		return err
	}

	return nil
}

func makeTree(ctx context.Context, repo *repository.Repository, nodeID restic.ID, dir string, includePaths []string) (newNodeID restic.ID, err error) {
	// If 0 paths are in includePaths, we can return the given nodeID as we know this and any sub-trees are not going to be modified
	if len(includePaths) == 0 {
		return nodeID, nil
	}
	// Load the tree for the given nodeID
	curTree, err := restic.LoadTree(ctx, repo, nodeID)
	if err != nil {
		return restic.ID{}, err
	}
	// Create a newTreeJSONBuilder and a new restic tree, as we will need to create both
	tb := restic.NewTreeJSONBuilder()
	tree := restic.NewTree(len(curTree.Nodes) + len(includePaths))
	// Mark any includePaths which are not either in this directory, or a sub-dir of this directory for removal
	remove := make([]bool, len(includePaths))
	for j := range remove {
		if !strings.HasPrefix(includePaths[j], dir) {
			remove[j] = true
		} else {
			remove[j] = false
		}
	}
	// Iterate over all the nodes for the current tree
	for i, node := range curTree.Nodes {
		// skipNode will be true if a file is modified or deleted
		skipNode := false
		for j, path := range includePaths {
			// If path has been marked for removal by another node, or from the above check, skip it
			if remove[j] {
				continue
			}
			// Ensure we ignore blank paths
			if path == "" {
				continue
			}
			// Check if the current included path matches the full path of the node we are checking
			if dir+node.Name == path {
				// Path was either modified or deleted
				_, err := os.Stat(path)
				skipNode = true
				// If the path doesn't exist on the FS, we can skip doing anything for the path
				if os.IsNotExist(err) {
					Verbosef("path %s was deleted on the FS, skipping adding it to the tree\n", path)
					continue
				}
				Verbosef("path %s was modified on the FS, generating a new node and adding it to the tree\n", path)
			}
			// If the current path lives in the same dir as the current node, and the current nodes name is greater than the current paths,
			// we want to insert a new node for our file in before the current node from the old tree
			if skipNode || (filepath.Dir(dir+node.Name) == filepath.Dir(path) && node.Name > filepath.Base(path)) {
				if !skipNode {
					Verbosef("path %s is new on the FS, generating a new node and adding it to the tree\n", path)
				}
				err = addNode(path, tb, tree)
				if err != nil {
					return restic.ID{}, err
				}
				// Mark file for removal so it's not included in the next recursive call to makeTree
				remove[j] = true
			}
		}
		// If skipNode is true, then don't insert the current node from the old tree into the new one, as we already did this above
		if !skipNode {
			// This piece is taken from the rewrite section
			if node.Type != "dir" {
				err := tree.Insert(node)
				if err != nil {
					return restic.ID{}, err
				}
				err = tb.AddNode(node)
				if err != nil {
					return restic.ID{}, err
				}
				continue
			}
			var subtree restic.ID
			if node.Subtree != nil {
				subtree = *node.Subtree
			}
			var keys []string
			for j, path := range includePaths {
				if !remove[j] {
					keys = append(keys, path)
				}
			}
			sort.Strings(keys)
			// If the current node is a dir, recurse into makeTree with the current nodes subtree, and iter over that tree
			newID, err := makeTree(ctx, repo, subtree, dir+node.Name+"/", keys)
			if err != nil {
				return restic.ID{}, err
			}
			// We will have a new resticID for the subtree, so apply it here
			node.Subtree = &newID
			err = tree.Insert(node)
			if err != nil {
				return restic.ID{}, err
			}
			err = tb.AddNode(node)
			if err != nil {
				return restic.ID{}, err
			}
			// If we are on the last iteration for the current tree, we need to check if any nodes need to be inserted at the end of the tree
			if i == len(curTree.Nodes)-1 {
				for _, path := range keys {
					// If the parent dir matches for any remaining paths in includePaths, we know they need to be inserted here, as all
					// other nodes in this dir would have been inserted into the tree and removed from the slice in the earlier section
					if filepath.Dir(dir+node.Name) == filepath.Dir(path) {
						Verbosef("path %s is new on the FS, generating a new node and adding it to the tree\n", path)
						err = addNode(path, tb, tree)
						if err != nil {
							return restic.ID{}, err
						}
					}
				}
			}
		}
	}
	// Get the JSON for the tree
	treeJSON, err := tb.Finalize()
	if err != nil {
		return restic.ID{}, err
	}

	// Save new tree
	id, err := restic.SaveTree(ctx, repo, tree)
	if err != nil {
		return restic.ID{}, err
	}
	// Save the JSON
	_, err = repo.SaveUnpacked(ctx, backend.PackFile, treeJSON)
	if err != nil {
		return restic.ID{}, err
	}
	// Return the resticID of the tree
	return id, nil
}

func runIncremental(ctx context.Context, opts IncrementalOptions, gopts GlobalOptions) error {
	if opts.includePatternOptions.Empty() && opts.Metadata.empty() {
		return errors.Fatal("Nothing to do: no includes provided and no new metadata provided")
	}

	var (
		repo   *repository.Repository
		unlock func()
		err    error
	)

	ctx, repo, unlock, err = openWithAppendLock(ctx, gopts, false)
	if err != nil {
		return err
	}
	defer unlock()

	bar := newIndexProgress(gopts.Quiet, gopts.JSON)
	if err = repo.LoadIndex(ctx, bar); err != nil {
		return err
	}

	// Get the latest snapshot as we will be "cloning" it to get the previous backup data
	sn, _, err := opts.SnapshotFilter.FindLatest(ctx, repo, repo, "latest")
	if err != nil {
		return err
	}

	Verbosef("loaded snapshot %v\n", sn.ID().Str())

	wg, ctx := errgroup.WithContext(ctx)
	repo.StartPackUploader(ctx, wg)

	// Get a list of all the paths which have changed (new files, modified files, deleted files)
	includePaths, err := opts.includePatternOptions.GetPathsFromPatterns()
	if err != nil {
		return err
	}

	Verbosef("got %d paths from include patterns\n", len(includePaths))

	// Sort the paths to ensure we insert nodes in the correct order when building the tree
	sort.Strings(includePaths)

	// Call the recursive makeTree funciton, which makes the tree with the changed files included (or excluded for deletions)
	treeID, err := makeTree(ctx, repo, *sn.Tree, "/", includePaths)
	if err != nil {
		return err
	}

	err = repo.Flush(ctx)
	if err != nil {
		return err
	}
	// Create a new snapshot with the previous snapshots data
	newSnapshot, err := restic.NewSnapshot(sn.Paths, sn.Tags, sn.Hostname, time.Now())
	if err != nil {
		return err
	}
	// Set the tree to the newly created TreeID
	newSnapshot.Tree = &treeID
	// Save the new snapshot
	id, err := restic.SaveSnapshot(ctx, repo, newSnapshot)
	if err != nil {
		return err
	}
	Verbosef("saved new snapshot %v\n", id.Str())

	return nil
}
