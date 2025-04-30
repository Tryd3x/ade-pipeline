#!/bin/bash
# Check for requirements.txt and install if found
if [[ -f "/opt/workspace/requirements.txt" ]]; then
    pip install -r /opt/workspace/requirements.txt
fi

# Run original command
exec "$@"