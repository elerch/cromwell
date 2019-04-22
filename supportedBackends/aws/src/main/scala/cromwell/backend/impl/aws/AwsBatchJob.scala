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

import cats.data.{Kleisli, ReaderT}
import cats.data.Kleisli._
import cats.data.ReaderT._
import cats.implicits._

import scala.language.higherKinds
import cats.effect.{Async, Timer}
import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model._
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.{GetLogEventsRequest, OutputLogEvent}
import cromwell.backend.BackendJobDescriptor
import cromwell.backend.io.JobPaths
import org.slf4j.LoggerFactory
import fs2.Stream
import software.amazon.awssdk.regions.Region

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

/** The actual job for submission in AWS batch. Currently, each job will
 *  have its own job definition and queue. Support for separation and reuse of job
 *  definitions will be provided later.
 *
 *  @constructor Create an AwsBatchJob object capable of submitting, aborting and monitoring itself
 *  @param jobDescriptor create a new person with a name and age.
 *  @param runtimeAttributes runtime attributes class (which subsequently pulls from config)
 *  @param commandLine command line to be passed to the job
 */
final case class AwsBatchJob(jobDescriptor: BackendJobDescriptor, // WDL/CWL
                             runtimeAttributes: AwsBatchRuntimeAttributes, // config or WDL/CWL
                             commandLine: String, // WDL/CWL
                             script: String, // WDL/CWL
                             dockerRc: String, // Calculated from StandardAsyncExecutionActor
                             dockerStdout: String, // Calculated from StandardAsyncExecutionActor
                             dockerStderr: String, // Calculated from StandardAsyncExecutionActor
                             inputs: Set[AwsBatchInput],
                             outputs: Set[AwsBatchFileOutput],
                             jobPaths: JobPaths, // Based on config, calculated in Job Paths, key to all things outside container
                             parameters: Seq[AwsBatchParameter],
                             configRegion: Option[Region]
                             ) {

  // TODO: This should be config
  val s3CommandKeyPrefix = "mycommandKey"
  val s3CommandKeyBucket = "emil"

  val Log = LoggerFactory.getLogger(AwsBatchJob.getClass)
  // TODO: Auth, endpoint
  lazy val client = {
   val builder = BatchClient.builder()
   configRegion.foreach(builder.region)
   builder.build
  }
  lazy val logsclient = {
    val builder = CloudWatchLogsClient.builder()
    configRegion.foreach(builder.region)
    builder.build
  }

  lazy val reconfiguredScript = {
    // The idea here is to inject a statically linked docker executable
    // Docker builds these and makes these available here: https://download.docker.com/linux/static/stable/x86_64/
    // We can control this by dowloading and tar extract to
    // tar -O -xf docker-18.09.5.tgz docker/docker > path-to-docker-exec
    //
    // Since docker is available via volume mounts:
    //
    // -v path-to-static-docker:/usr/bin/docker:ro
    // -v /var/run/docker.sock:/var/run/docker.sock:ro
    //
    // We can use docker to access whatever else we might need in the future
    // in this case, we want the aws cli to be able to pull in the command
    // text from s3
    //
    // We're exec-ing the command from memory, but this might be better off
    // written to disk and executed? If we are able to do this, we can probably
    // use !/bin/sh instead of !/bin/bash
    //
    // This is the nextflow command:
    // bash -o pipefail -c trap "{ ret=$?; /home/ec2-user/miniconda/bin/aws s3 cp --only-show-errors .command.log s3://aws-nextflow-demo-us-east-2/runs/e7/eb6fdba31092b6d4cfb66326dc011a/.command.log%7C%7Ctrue; exit $ret; }" EXIT; /home/ec2-user/miniconda/bin/aws s3 cp --only-show-errors s3://aws-nextflow-demo-us-east-2/runs/e7/eb6fdba31092b6d4cfb66326dc011a/.command.run - | bash 2>&1 | tee .command.log
    //
    // However, we **REJECT** this command because the script passed into this
    // by Cromwell is mostly responsible for all the pipefail/tee/etc
    s3CommandKeyBucket match {
      case bucket if bucket == "" => script
      case bucket if bucket != "" => s"""
      |!/bin/sh
      |
      |cmd=$$(docker run awscli:latest aws s3 cp s3://$s3CommandKeyBucket/$s3CommandKeyPrefix/)
      |tmp=$(mktemp)
      |echo $$cmd >> $$tmp
      |chmod +x $$tmp
      |$$tmp
      """
    }
  }
  def submitJob[F[_]]()(
                       implicit timer: Timer[F],
                       async: Async[F]): Aws[F, SubmitJobResponse] = {
    val taskId = jobDescriptor.key.call.fullyQualifiedName + "-" + jobDescriptor.key.index + "-" + jobDescriptor.key.attempt
    val workflow = jobDescriptor.workflowDescriptor
    val uniquePath = workflow.callable.name + "/" +
                     jobDescriptor.taskCall.callable.name + "/" +
                     workflow.id + "/" +
                     jobDescriptor.key.index + "/" +
                     jobDescriptor.key.attempt
    Log.info(s"""Submitting job to AWS Batch""")
    Log.info(s"""dockerImage: ${runtimeAttributes.dockerImage}""")
    Log.info(s"""jobQueueArn: ${runtimeAttributes.queueArn}""")
    Log.info(s"""taskId: $taskId""")
    Log.info(s"""hostpath root: $uniquePath""")

    def callClient(definitionArn: String, awsBatchAttributes: AwsBatchAttributes): Aws[F, SubmitJobResponse] = {
      val submit =
        async.delay(client.submitJob(
          SubmitJobRequest.builder()
            .jobName(sanitize(jobDescriptor.taskCall.fullyQualifiedName))
            .parameters(parameters.collect({ case i: AwsBatchInput => i.toStringString }).toMap.asJava)
            .jobQueue(runtimeAttributes.queueArn)
            .jobDefinition(definitionArn).build
        ))
      ReaderT.liftF(
        Stream.retry(submit, 0.millis, duration => duration.plus(duration), awsBatchAttributes.submitAttempts.value, {
          // RegisterJobDefinition is eventually consistent, so it may not be there
          case e: ClientException => e.statusCode() == 404
          case _ => false
        }).compile.last.map(_.get)) //if successful there is guaranteed to be a value emitted, hence we can .get this option
    }

    (createDefinition[F](s"""${workflow.callable.name}-${jobDescriptor.taskCall.callable.name}""", uniquePath) product Kleisli.ask[F, AwsBatchAttributes]).
      flatMap((callClient _).tupled)
  }

  /** Creates a job definition in AWS Batch
   *
   *  @param name Job definition name
   *  @return Arn for newly created job definition
   *
   */
  private def createDefinition[F[_]](name: String,
                                     taskId: String)(
                                     implicit async: Async[F],
                                     timer: Timer[F]): Aws[F, String] = ReaderT { awsBatchAttributes =>
    val jobDefinitionBuilder = StandardAwsBatchJobDefinitionBuilder
    val jobDefinitionContext = AwsBatchJobDefinitionContext(runtimeAttributes,
                                                            taskId,
                                                            reconfiguredScript,
                                                            dockerRc,
                                                            dockerStdout,
                                                            dockerStderr,
                                                            jobDescriptor,
                                                            jobPaths,
                                                            inputs,
                                                            outputs,
                                                            s3CommandKeyPrefix,
                                                            s3CommandKeyBucket)

    val jobDefinition = jobDefinitionBuilder.build(jobDefinitionContext)

    // See:
    //
    // http://aws-java-sdk-javadoc.s3-website-us-west-2.amazonaws.com/latest/software/amazon/awssdk/services/batch/model/RegisterJobDefinitionRequest.Builder.html
    val definitionRequest = RegisterJobDefinitionRequest.builder
                              .containerProperties(jobDefinition.containerProperties)
                              .jobDefinitionName(sanitize(name))
                              // See https://stackoverflow.com/questions/24349517/scala-method-named-type
                              .`type`(JobDefinitionType.CONTAINER)
                              .build

    val submit = async.delay(client.registerJobDefinition(definitionRequest).jobDefinitionArn)

    val retry: F[String] = Stream.retry(submit, 0.millis, _ * 2, awsBatchAttributes.createDefinitionAttempts.value, {
      // RegisterJobDefinition throws 404s every once in a while
      case e: ClientException => e.statusCode() == 404
      case _ => false
    }).compile.last.map(_.get)

    async.recoverWith(retry) {
      case e: ClientException if e.statusCode() == 409 =>
        // This could be a problem here, as we might have registerJobDefinition
        // but not describeJobDefinitions permissions. We've changed this to a
        // warning as a result of this potential
        Log.warn("Job definition already exists. Performing describe and retrieving latest revision.")
        async.
          delay(client.describeJobDefinitions(DescribeJobDefinitionsRequest.builder().jobDefinitionName(sanitize(name)).build())).
          map(_.jobDefinitions().asScala).
          map(_.last.jobDefinitionArn())
    }
  }

  /** Sanitizes a job and job definition name
    *
    *  @param name Job or Job definition name
    *  @return Sanitized name
    *
    */
  private def sanitize(name: String): String =
    // Up to 128 letters (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
    // We'll replace all invalid characters with an underscore
    name.replaceAll("[^A-Za-z0-9_\\-]", "_").slice(0,128)


  /** Gets the status of a job by its Id, converted to a RunStatus
   *
   *  @param jobId Job ID as defined in AWS Batch
   *  @return Current RunStatus
   *
   */
  def status(jobId: String): Try[RunStatus] = for {
    statusString <- Try(detail(jobId).status)
    runStatus <- RunStatus.fromJobStatus(statusString, jobId)
  } yield runStatus

  def detail(jobId: String): JobDetail = {
    //TODO: This client call should be wrapped in a cats Effect
     val describeJobsResponse = client.describeJobs(DescribeJobsRequest.builder.jobs(jobId).build)

     describeJobsResponse.jobs.asScala.headOption.
       getOrElse(throw new RuntimeException(s"Expected a job Detail to be present from this request: $describeJobsResponse and this response: $describeJobsResponse "))
  }

  //TODO: unused at present
  def rc(detail: JobDetail): Integer = {
     detail.container.exitCode
  }

  def output(detail: JobDetail): String = {
     val events: Seq[OutputLogEvent] = logsclient.getLogEvents(GetLogEventsRequest.builder
                                            // http://aws-java-sdk-javadoc.s3-website-us-west-2.amazonaws.com/latest/software/amazon/awssdk/services/batch/model/ContainerDetail.html#logStreamName--
                                            .logGroupName("/aws/batch/job")
                                            .logStreamName(detail.container.logStreamName)
                                            .startFromHead(true)
                                            .build).events.asScala.toSeq
     val eventMessages = for ( event <- events ) yield event.message
     eventMessages mkString "\n"
  }

  //TODO: Wrap in cats Effect
  def abort(jobId: String): CancelJobResponse = {
    client.cancelJob(CancelJobRequest.builder.jobId(jobId).reason("cromwell abort called").build)
  }
}
