#!/usr/bin/env bash
# Run the Plenum First Said pipeline and post queue

cd "$(dirname "$0")/parser"

# Pipeline: discover new protocols, extract text, find new words
uv run python plenar.py

# Post one word from the queue
uv run python post_queue.py
