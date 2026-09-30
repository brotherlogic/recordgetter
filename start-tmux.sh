#!/bin/bash

# Ensure the 'recordgetter' session exists
if ! tmux has-session -t recordgetter 2>/dev/null; then
  # Create a new session named 'recordgetter', detached
  cd /workspaces/recordgetter
  tmux new-session -d -s recordgetter
  
  # Split the window horizontally (-h)
  # The left pane will remain a terminal
  # The right pane will run 'gh dash'
  tmux split-window -h -t recordgetter "gh dash"
fi
