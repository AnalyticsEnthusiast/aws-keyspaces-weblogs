#!/bin/bash

STACK_NAME=
PROFILE=
STACK_TEMPLATE=
STACK_PARAMETERS=

usage() { echo "Usage: $0 [-s my-stack-name] [-p profile-name]" 1>&2; exit 1; }

while getopts "s:p:h" arg; do
  case $arg in
    h)
        echo "usage"
        usage
        ;;
    s)
        STACK_NAME="$OPTARG"
        ;;
    p)
        PROFILE="$OPTARG"
        ;;
    t)
        STACK_TEMPLATE="$OPTARG"
        ;;
    P)
        STACK_PARAMETERS="$OPTARG"
        ;;
  esac
done
shift $((OPTIND-1))

if [ -z "$STACK_NAME" ] || [ -z "$PROFILE" ] || [ -z "$STACK_TEMPLATE" ] || [ -z "$STACK_PARAMTERS" ]; then
    usage
fi



aws cloudformation create-stack \
    --stack-name ${STACK_NAME} \
    --template-body file://stacks/${STACK_TEMPLATE} \
    --parameters file://parameters/${STACK_PARAMETERS} \
    --profile ${PROFILE}