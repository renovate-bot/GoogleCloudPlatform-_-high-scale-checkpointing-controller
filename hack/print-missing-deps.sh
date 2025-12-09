#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# this example shows how to mount local ramdisk to a workload pod


set -o errexit
set -o nounset
set -o pipefail

echo "Verifying Docker Executables have appropriate dependencies"

printMissingDep() {
  if /usr/bin/ldd "$@" 2>&1 | grep -q "not found"; then
    echo "!!! Missing deps for $@ !!!"
    # Run it again without -q so we see WHICH file is missing
    ldd "$@" 2>&1 | grep "not found"
    exit 1
  fi
}

export -f printMissingDep

/usr/bin/find / -path /proc -prune -o -type f -executable -print | /usr/bin/xargs -I {} bash -c 'printMissingDep "{}"'
