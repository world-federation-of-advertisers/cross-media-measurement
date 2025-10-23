#!/usr/bin/env python3
"""
Script to query and decode WorkItems from Spanner

Usage:
    ./view_workitems.py [limit]

Examples:
    ./view_workitems.py         # Show last 20 work items
    ./view_workitems.py 50      # Show last 50 work items
"""

import base64
import re
import subprocess
import sys

# Configuration
INSTANCE = "dev-instance"
DATABASE = "secure-computation"
PROJECT = "halo-cmm-dev"

STATE_MAP = {
    0: "STATE_UNSPECIFIED",
    1: "QUEUED",
    2: "RUNNING",
    3: "SUCCEEDED",
    4: "FAILED",
}

def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20

    print("=" * 70)
    print("WorkItems from Spanner")
    print(f"Database: {DATABASE}")
    print(f"Instance: {INSTANCE}")
    print(f"Project: {PROJECT}")
    print("=" * 70)
    print()

    # Query WorkItems
    query = f"""
    SELECT WorkItemId, WorkItemResourceId, QueueId, State, WorkItemParams,
           CreateTime, UpdateTime
    FROM WorkItems
    ORDER BY CreateTime DESC
    LIMIT {limit}
    """

    # Execute gcloud command
    cmd = [
        "gcloud", "spanner", "databases", "execute-sql", DATABASE,
        "--instance", INSTANCE,
        "--project", PROJECT,
        "--sql", query
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output_lines = result.stdout.strip().split('\n')
    except subprocess.CalledProcessError as e:
        print(f"Error executing query: {e}")
        print(e.stderr)
        sys.exit(1)

    if len(output_lines) <= 1:
        print("No WorkItems found.")
        return

    # Parse the table output
    # Skip the header line
    data_lines = output_lines[1:]

    work_items = []
    for line in data_lines:
        if not line.strip():
            continue

        # Split by multiple spaces (table columns are space-separated)
        parts = re.split(r'\s{2,}', line.strip())
        if len(parts) >= 7:
            work_items.append({
                'id': parts[0],
                'resource_id': parts[1],
                'queue_id': parts[2],
                'state': int(parts[3]),
                'params': parts[4],
                'create_time': parts[5],
                'update_time': parts[6],
            })

    # Display each work item
    for idx, item in enumerate(work_items, 1):
        state_name = STATE_MAP.get(item['state'], 'UNKNOWN')

        print("=" * 70)
        print(f"WorkItem #{idx}")
        print("=" * 70)
        print(f"ID: {item['id']}")
        print(f"Resource ID: {item['resource_id']}")
        print(f"Queue ID: {item['queue_id']}")
        print(f"State: {item['state']} ({state_name})")
        print(f"Created: {item['create_time']}")
        print(f"Updated: {item['update_time']}")
        print()
        print("--- Decoded Parameters ---")
        print()

        # Decode params
        if item['params'] and item['params'] not in ('null', 'None', ''):
            try:
                decoded_bytes = base64.b64decode(item['params'])
                decoded_str = decoded_bytes.decode('utf-8', errors='ignore')

                # Extract key information using regex
                # Clean up the decoded string to only include printable chars
                decoded_clean = ''.join(c for c in decoded_str if c.isprintable() or c in '\n\t')

                data_provider_match = re.search(r'dataProviders/[A-Za-z0-9_-]+(?![/])', decoded_clean)
                storage_match = re.search(r'gs://[a-z0-9-]+', decoded_clean)
                project_match = re.search(r'halo-[a-z0-9-]+', decoded_clean)
                cert_match = re.search(r'dataProviders/[^/]+/certificates/[A-Za-z0-9_-]+', decoded_clean)
                req_uuid = re.search(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', decoded_clean)

                # Extract cert paths more carefully
                cert_paths = []
                for match in re.finditer(r'/tmp/edp_certs/[a-z0-9_]+\.(der|pem|key|tink)', decoded_clean):
                    path = match.group(0)
                    if path not in cert_paths:
                        cert_paths.append(path)

                if data_provider_match:
                    print(f"  Data Provider: {data_provider_match.group(0)}")
                if storage_match:
                    print(f"  Storage Bucket: {storage_match.group(0)}")
                if project_match:
                    print(f"  GCP Project: {project_match.group(0)}")
                if cert_match:
                    print(f"  Certificate: {cert_match.group(0)}")

                if cert_paths:
                    print()
                    print("  Certificate Paths (in TEE):")
                    for path in cert_paths:
                        print(f"    - {path}")

                print()
                if req_uuid:
                    req_path = f"gs://secure-computation-storage-dev-bucket/edp7/requisitions/{req_uuid.group(0)}"
                    print(f"  Requisition Path:")
                    print(f"    {req_path}")
                    print()
                    print(f"  >>> Requisition UUID: {req_uuid.group(0)} <<<")

            except Exception as e:
                print(f"  Error decoding params: {e}")
        else:
            print("  (No params)")

        print()

    # Summary
    print()
    print("=" * 70)
    print(f"Total WorkItems: {len(work_items)}")
    print("=" * 70)

    # State breakdown
    print()
    print("State Breakdown:")
    state_counts = {}
    for item in work_items:
        state = item['state']
        state_counts[state] = state_counts.get(state, 0) + 1

    for state, count in sorted(state_counts.items()):
        state_name = STATE_MAP.get(state, 'UNKNOWN')
        print(f"  State {state} ({state_name}): {count} item(s)")

if __name__ == "__main__":
    main()
