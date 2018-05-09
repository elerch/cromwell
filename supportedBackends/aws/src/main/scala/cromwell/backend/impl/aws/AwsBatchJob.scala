/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in the
 *  documentation and/or other materials provided with the distribution.
 *
 *  3. Neither the name of the copyright holder nor the names of its
 *  contributors may be used to endorse or promote products derived from
 *  this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 *  BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 *  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 *  THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 *  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 *  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 *  STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
package cromwell.backend.impl.aws

import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.
                                        {
                                          CancelJobRequest,
                                          CancelJobResponse,
                                          RegisterJobDefinitionRequest,
                                          SubmitJobRequest,
                                          SubmitJobResponse,
                                          DescribeJobsRequest,
                                          JobDefinitionType,
                                          JobDetail
                                        }
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest
import cromwell.backend.BackendJobDescriptor
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

object AwsBatchJob

/** The actual job for submission in AWS batch. Currently, each job will
 *  have its own job definition and queue. Support for separation and reuse of job
 *  definitions will be provided later.
 *
 *  @constructor Create an AwsBatchJob object capable of submitting, aborting and monitoring itself
 *  @param jobDescriptor create a new person with a name and age.
 *  @param runtimeAttributes runtime attributes class (which subsequently pulls from config)
 *  @param commandLine command line to be passed to the job
 */
final case class AwsBatchJob(jobDescriptor: BackendJobDescriptor,           // WDL
                             runtimeAttributes: AwsBatchRuntimeAttributes,  // config
                             commandLine: String,                           // WDL
                             script: String,        // WDL
                             parameters: Seq[AwsBatchParameter]
                             ) {

  val Log = LoggerFactory.getLogger(AwsBatchJob.getClass)

  // TODO: Auth, endpoint
  lazy val client = BatchClient.builder()
                   // .credentialsProvider(...)
                   // .endpointOverride(...)
                   .build
  lazy val logsclient = CloudWatchLogsClient.builder()
                   // .credentialsProvider(...)
                   // .endpointOverride(...)
                   .build

  def submitJob(): Try[SubmitJobResponse] = Try {
    Log.info(s"""Submitting job to AWS Batch""")
    Log.info(s"""dockerImage: ${runtimeAttributes.dockerImage}""")
    Log.info(s"""jobQueueArn: ${runtimeAttributes.queueArn}""")
    Log.info(s"""commandLine: $commandLine""")
    // Log.info(s"""logFileName: $logFileName""")

    // runtimeAttributes
    // dockerImage ceomse from the WDL task definition
    // commandList
    lazy val workflow = jobDescriptor.workflowDescriptor

    // Build the Job definition before we submit. Eventually this should be
    // done separately and cached.
    val definitionArn = createDefinition(workflow.callable.name)

    val job = client.submitJob(SubmitJobRequest.builder()
                .jobName(workflow.callable.name)
                .parameters(parameters.collect({ case i: AwsBatchInput => i.toStringString }).toMap.asJava)
                .jobQueue(runtimeAttributes.queueArn)
                .jobDefinition(definitionArn).build)

    // TODO: Remove the following comment: disks cannot have mount points at runtime, so set them null
    // TODO: Others have an "ephemeral pipeline". AWS Batch does not have the same concept,
    //       so in this case we need to register a new job description, then call submitjob to run it.

    job
  }

  /** Creates a job definition in AWS Batch
   *
   *  @param name Job definition name
   *  @return Arn for newly created job definition
   *
   */
  private def createDefinition(name: String): String = {
    val jobDefinitionBuilder = StandardAwsBatchJobDefinitionBuilder
    val jobDefinition = jobDefinitionBuilder.build(commandLine, runtimeAttributes, runtimeAttributes.dockerImage)

    // See:
    //
    // http://aws-java-sdk-javadoc.s3-website-us-west-2.amazonaws.com/latest/software/amazon/awssdk/services/batch/model/RegisterJobDefinitionRequest.Builder.html
    val definitionRequest = RegisterJobDefinitionRequest.builder
                              .containerProperties(jobDefinition.containerProperties)
                              .jobDefinitionName(name)
                              // See https://stackoverflow.com/questions/24349517/scala-method-named-type
                              .`type`(JobDefinitionType.CONTAINER)
                              .build

    client.registerJobDefinition(definitionRequest).jobDefinitionArn
  }

  /** Gets the status of a job by it's Id, converted to a RunStatus
   *
   *  @param jobId Job ID as defined in AWS Batch
   *  @return Current RunStatus
   *
   */
  def status(jobId: String): Try[RunStatus] = {
     RunStatus.fromJobStatus(detail(jobId).status, jobId)
  }

  def detail(jobId: String): JobDetail = {
     val describeJobsResponse = client.describeJobs(DescribeJobsRequest.builder.jobs(jobId).build)
     val jobs = describeJobsResponse.jobs.asScala.toSeq
     jobs(0)
  }
  def rc(detail: JobDetail): Integer = {
     detail.container.exitCode
  }

  def output(detail: JobDetail): String = {
     // List<OutputEvent>
     val events = logsclient.getLogEvents(GetLogEventsRequest.builder
                                            // http://aws-java-sdk-javadoc.s3-website-us-west-2.amazonaws.com/latest/software/amazon/awssdk/services/batch/model/ContainerDetail.html#logStreamName--
                                            .logGroupName("/aws/batch/job")
                                            .logStreamName(detail.container.logStreamName)
                                            .startFromHead(true)
                                            .build).events.asScala.toSeq
     val eventMessages = for ( event <- events ) yield event.message
     eventMessages mkString "\n"
  }

  def abort(jobId: String): CancelJobResponse = {
    client.cancelJob(CancelJobRequest.builder.jobId(jobId).reason("cromwell abort called").build)
  }
}
