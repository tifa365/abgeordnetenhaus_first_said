#!/usr/bin/env bash
# Setup cron jobs for Plenum First Said bot

BOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARSER_DIR="$BOT_DIR/parser"

# Pipeline runs every Monday at 6:00 AM to discover new protocols
# (Plenary sessions are almost always on Thursdays, every 2 weeks)
CRON_PIPELINE="0 6 * * 1 cd $PARSER_DIR && uv run python plenar.py >> $PARSER_DIR/plenar.log 2>&1"

# Post one word every 1-2 hours from 8-22
CRON_POST="0 8-22/1 * * * cd $PARSER_DIR && uv run python post_queue.py >> $PARSER_DIR/post.log 2>&1"

if crontab -l 2>/dev/null | grep -q "plenum_first_said.*plenar.py"; then
    echo "Cron jobs already exist. Remove them first with: crontab -e"
else
    (crontab -l 2>/dev/null; echo "$CRON_PIPELINE"; echo "$CRON_POST") | crontab -
    echo "Cron jobs added successfully!"
    echo "  Pipeline:  daily at 06:00"
    echo "  Posting:   every 2h from 08:00-22:00"
    echo ""
    echo "Logs: $PARSER_DIR/plenar.log, $PARSER_DIR/post.log"
    echo "Edit: crontab -e"
fi
