#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements; and to You under the Apache License, Version 2.0.

./copyJMXFiles.sh

export SCHEDULER_OPTS
SCHEDULER_OPTS="$SCHEDULER_OPTS $(./transformEnvironment.sh)"

exec scheduler/bin/scheduler "$@"