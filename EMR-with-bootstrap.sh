#!/bin/bash

usage(){ 
    echo "Usage $0 [-n|name] [-k|key] [-s|subnet] [-p|profile] [-b|bucket Name] [-h|help]" 
    exit 0
}

CLUSTER_NAME=
KEY_NAME=
SUBNET_NAME=
PROFILE_NAME=
CLUSTER_ID=
BUCKET_NAME=
S3_CODE_BUCKET=
while getopts ":n:k:s:p:b:c:h" arg
do
    case "$arg" in
        n)
            CLUSTER_NAME="${OPTARG}"
            ;;
        k)
            KEY_NAME="${OPTARG}"
            ;;
        s)
            SUBNET_NAME="${OPTARG}"
            ;;
        p)
            PROFILE_NAME="${OPTARG}"
            ;;
        b)
            BUCKET_NAME="${OPTARG}"
            ;;
        c)
            S3_CODE_BUCKET="${OPTARG}"
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac

done
shift $((OPTIND-1))


# Make sure all information is provided
if [ -z "${CLUSTER_NAME}" ] || [ -z "${KEY_NAME}" ] || [ -z "${SUBNET_NAME}" ] || [ -z "${PROFILE_NAME}" ] || [ -z "${BUCKET_NAME}" ] || [ -z "${S3_CODE_BUCKET}" ];
then
    usage
else
    echo "${CLUSTER_NAME}"
    echo "${KEY_NAME}"
    echo "${SUBNET_NAME}"
    echo "${PROFILE_NAME}"
    echo "${BUCKET_NAME}"
    echo "${S3_CODE_BUCKET}"
fi


callEMRWithBootstrap(){

    aws emr create-cluster \
    --name "${CLUSTER_NAME}" \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName="${KEY_NAME}",SubnetId="${SUBNET_NAME}" \
    --instance-type m5.xlarge \
    --profile "${PROFILE_NAME}" \
    --bootstrap-actions Path="s3://${S3_CODE_BUCKET}/bootstrap.sh",Args=[${BUCKET_NAME},${S3_CODE_BUCKET}]
    
}

main(){
    
    callEMRWithBootstrap

}

main