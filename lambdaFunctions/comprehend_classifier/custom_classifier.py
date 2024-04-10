import json
import boto3
import os
import time
import logging
import sys
import tarfile
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel("INFO")

class ComprehendClassifier():
    """Encapsulates an Amazon Comprehend custom classifier."""
    
    comprehend_client = ''
    classifier_arn = ''

    def __init__(self, comprehend_arn):
        """
        :param comprehend_client: A Boto3 Comprehend client.
        """
        
        # Instantiates necessary AWS clients
        session = boto3.Session()
        self.comprehend_client = session.client('comprehend')
        self.classifier_arn = comprehend_arn


    def start_job(
        self,
        job_name,
        input_bucket,
        input_key,
        input_format,
        output_bucket,
        output_key,
        data_access_role_arn,
    ):
        """
        Starts a classification job. The classifier must be trained or the job
        will fail. Input is read from the specified Amazon S3 input bucket and
        written to the specified output bucket. Output data is stored in a tar
        archive compressed in gzip format. The job runs asynchronously, so you can
        call `describe_document_classification_job` to get job status until it
        returns a status of SUCCEEDED.

        :param job_name: The name of the job.
        :param input_bucket: The Amazon S3 bucket that contains input data.
        :param input_key: The prefix used to find input data in the input
                          bucket. If multiple objects have the same prefix, all
                          of them are used.
        :param input_format: The format of the input data, either one document per
                             file or one document per line.
        :param output_bucket: The Amazon S3 bucket where output data is written.
        :param output_key: The prefix prepended to the output data.
        :param data_access_role_arn: The Amazon Resource Name (ARN) of a role that
                                     grants Comprehend permission to read from the
                                     input bucket and write to the output bucket.
        :return: Information about the job, including the job ID.
        """
        try:
            response = self.comprehend_client.start_document_classification_job(
                DocumentClassifierArn=self.classifier_arn,
                JobName=job_name,
                InputDataConfig={
                    "S3Uri": f"s3://{input_bucket}/{input_key}/",
                    "InputFormat": input_format,
                },
                OutputDataConfig={"S3Uri": f"s3://{output_bucket}/{output_key}/"},
                DataAccessRoleArn=data_access_role_arn,
            )
            print(
                "Document classification job {} is {}.".format(job_name, response["JobStatus"]) 
            )
        except ClientError:
            print("Couldn't start classification job %s.", job_name)
            raise
        else:
            return response
    
    def describe_job(self, job_id):
        """
        Gets metadata about a classification job.

        :param job_id: The ID of the job to look up.
        :return: Metadata about the job.
        """
        try:
            response = self.comprehend_client.describe_document_classification_job(
                JobId=job_id
            )
            job = response["DocumentClassificationJobProperties"]
            #print("Got classification job %s.", job["JobName"])
            return job
        except ClientError:
            print("Couldn't get classification job %s.", job_id)
            raise
        else:
            return job

    # snippet-end:[python.example_code.comprehend.DescribeDocumentClassificationJob]

    # snippet-start:[python.example_code.comprehend.ListDocumentClassificationJobs]
    def list_jobs(self):
        """
        Lists the classification jobs for the current account.

        :return: The list of jobs.
        """
        try:
            response = self.comprehend_client.list_document_classification_jobs()
            jobs = response["DocumentClassificationJobPropertiesList"]
            print("Got %s document classification jobs.", len(jobs))
        except ClientError:
            print(
                "Couldn't get document classification jobs.",
            )
            raise
        else:
            return jobs