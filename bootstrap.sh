#!/bin/bash

usage() { 
        echo "Usage $0 [-b| bucket_name] [-h|help]" 
        exit 0
}

bucket_name="$1"
s3_code_bucket="$2"

#while getopts "b:h" o;
#do
#    case "${o}" in
#    b)
#        bucket_name="${OPTARG}"
#    ;;
#    h)
#        usage
#    ;;
#    *)
#        usage
#    ;;
#    esac
#done
#shift $(($OPTIND-1))

if [ -z "${bucket_name}" ];
then
    usage
fi

sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install findspark
aws s3 cp s3://${s3_code_bucket}/ /home/hadoop/ --recursive
cp /home/hadoop/aws-template.cfg /home/hadoop/aws.cfg

#Populate dl.cfg
sed -i "s/S3_BUCKET_NAME=/S3_BUCKET_NAME=${bucket_name}/" /home/hadoop/dl.cfg
sed -i "s|S3_DESTINATION=s3://|S3_DESTINATION=s3://${bucket_name}/|" /home/hadoop/dl.cfg

source /home/hadoop/.bashrc