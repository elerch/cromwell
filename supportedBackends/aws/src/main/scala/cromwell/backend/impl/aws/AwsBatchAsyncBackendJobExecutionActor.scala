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

import java.net.SocketTimeoutException

import _root_.io.grpc.Status
import akka.actor.ActorRef
import common.util.StringUtil._
import common.validation.Validation._
import cromwell.backend._
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, PendingExecutionHandle}
import cromwell.backend.impl.aws.RunStatus.TerminalRunStatus
import cromwell.backend.impl.aws.io._
import cromwell.backend.io.DirectoryFunctions
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core._
import cromwell.core.logging.JobLogger
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.filesystems.aws.S3Path
import cromwell.filesystems.aws.batch.S3BatchCommandBuilder
import cromwell.services.keyvalue.KeyValueServiceActor._
import cromwell.services.keyvalue.KvClient
import org.slf4j.LoggerFactory
import wom.CommandSetupSideEffectFile
import wom.callable.Callable.OutputDefinition
import wom.core.FullyQualifiedName
import wom.expression.NoIoFunctionSet
import wom.types.{WomArrayType, WomSingleFileType}
import wom.values._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Success, Try}

object AwsBatchAsyncBackendJobExecutionActor {
  val AwsBatchOperationIdKey = "__aws_batch_operation_id"

  object WorkflowOptionKeys {
    val MonitoringScript = "monitoring_script"
  }

  type AwsBatchPendingExecutionHandle = PendingExecutionHandle[StandardAsyncJob, Run, RunStatus]

  def StandardException(errorCode: Status,
                        message: String,
                        jobTag: String,
                        returnCodeOption: Option[Int],
                        stderrPath: Path) = {
    val returnCodeMessage = returnCodeOption match {
      case Some(returnCode) if returnCode == 0 => "Job exited without an error, exit code 0."
      case Some(returnCode) => s"Job exit code $returnCode. Check $stderrPath for more information."
      case None => "The job was stopped before the command finished."
    }

    new Exception(s"Task $jobTag failed. $returnCodeMessage AWS Batch error code ${errorCode.getCode.value}. $message")
  }
}

class AwsBatchAsyncBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with AwsBatchJobCachingActorHelper
    with KvClient {

  override lazy val ioCommandBuilder = S3BatchCommandBuilder

  import AwsBatchAsyncBackendJobExecutionActor._

  val slf4jLogger = LoggerFactory.getLogger(AwsBatchAsyncBackendJobExecutionActor.getClass)
  val logger = new JobLogger("AwsBatchRun", jobDescriptor.workflowDescriptor.id, jobDescriptor.key.tag, None, Set(slf4jLogger))

  val backendSingletonActor: ActorRef =
    standardParams.backendSingletonActorOption.getOrElse(
      throw new RuntimeException("Batch backend actor cannot exist without the backend singleton actor"))

  override type StandardAsyncRunInfo = Run

  override type StandardAsyncRunStatus = RunStatus

  override lazy val pollBackOff = SimpleExponentialBackoff(1.second, 5.minutes, 1.1)

  // override lazy val pollBackOff = SimpleExponentialBackoff(
  //   initialInterval = 30 seconds, maxInterval = attributes.maxPollingInterval seconds, multiplier = 1.1)

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(
    initialInterval = 3 seconds, maxInterval = 20 seconds, multiplier = 1.1)

  private lazy val jobDockerImage = jobDescriptor.maybeCallCachingEligible.dockerHash.getOrElse(runtimeAttributes.dockerImage)

  override lazy val dockerImageUsed: Option[String] = Option(jobDockerImage)

  override def tryAbort(job: StandardAsyncJob): Unit = {
    Run(job).abort()
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  /**
    * Takes two arrays of remote and local WOM File paths and generates the necessary AwsBatchInputs.
    */
  private def inputsFromWomFiles(namePrefix: String,
                                    remotePathArray: Seq[WomFile],
                                    localPathArray: Seq[WomFile],
                                    jobDescriptor: BackendJobDescriptor): Iterable[AwsBatchInput] = {
    (remotePathArray zip localPathArray zipWithIndex) flatMap {
      case ((remotePath, localPath), index) =>
        Seq(AwsBatchFileInput(s"$namePrefix-$index", remotePath.valueString, DefaultPathBuilder.get(localPath.valueString), workingDisk))
    }
  }

  /**
    * Turns WomFiles into relative paths.  These paths are relative to the working disk.
    *
    * relativeLocalizationPath("foo/bar.txt") -> "foo/bar.txt"
    * relativeLocalizationPath("s3://some/bucket/foo.txt") -> "some/bucket/foo.txt"
    */
  private def relativeLocalizationPath(file: WomFile): WomFile = {
    file.mapFile(value =>
      getPath(value) match {
        case Success(path) => path.pathWithoutScheme
        case _ => value
      }
    )
  }

  private[aws] def generateAwsBatchInputs(jobDescriptor: BackendJobDescriptor): Set[AwsBatchInput] = {
    // We need to tell PAPI about files that were created as part of command instantiation (these need to be defined
    // as inputs that will be localized down to the VM). Make up 'names' for these files that are just the short
    // md5's of their paths.
    val writeFunctionFiles = instantiatedCommand.createdFiles map { f => f.file.value.md5SumShort -> List(f) } toMap

    def localizationPath(f: CommandSetupSideEffectFile) =
      f.relativeLocalPath.fold(ifEmpty = relativeLocalizationPath(f.file))(WomFile(f.file.womFileType, _))
    val writeFunctionInputs = writeFunctionFiles flatMap {
      case (name, files) => inputsFromWomFiles(name, files.map(_.file), files.map(localizationPath), jobDescriptor)
    }

    // Collect all WomFiles from inputs to the call.
    val callInputFiles: Map[FullyQualifiedName, Seq[WomFile]] = jobDescriptor.fullyQualifiedInputs mapValues {
      womFile =>
        val arrays: Seq[WomArray] = womFile collectAsSeq {
          case womFile: WomFile =>
            val files: List[WomSingleFile] = DirectoryFunctions
              .listWomSingleFiles(womFile, callPaths.workflowPaths)
              .toTry(s"Error getting single files for $womFile").get
            WomArray(WomArrayType(WomSingleFileType), files)
        }

        arrays.flatMap(_.value).collect {
          case womFile: WomFile => womFile
        }
    } map identity // <-- unlazy the mapValues

    val callInputInputs = callInputFiles flatMap {
      case (name, files) => inputsFromWomFiles(name, files, files.map(relativeLocalizationPath), jobDescriptor)
    }

    (writeFunctionInputs ++ callInputInputs).toSet

  }

  /**
    * Given a path (relative or absolute), returns a (Path, AwsBatchVolume) tuple where the Path is
    * relative to the Volume's mount point
    *
    * @throws Exception if the `path` does not live in one of the supplied `disks`
    */
  private def relativePathAndVolume(path: String, disks: Seq[AwsBatchVolume]): (Path, AwsBatchVolume) = {
    val absolutePath = DefaultPathBuilder.get(path) match {
      case p if !p.isAbsolute => AwsBatchWorkingDisk.MountPoint.resolve(p)
      case p => p
    }

    disks.find(d => absolutePath.startsWith(d.mountPoint)) match {
      case Some(disk) => (disk.mountPoint.relativize(absolutePath), disk)
      case None =>
        throw new Exception(s"Absolute path $path doesn't appear to be under any mount points: ${disks.map(_.toString).mkString(", ")}")
    }
  }

  private def makeSafeAwsBatchReferenceName(referenceName: String) = {
    if (referenceName.length <= 127) referenceName else referenceName.md5Sum
  }

  private[aws] def generateAwsBatchOutputs(jobDescriptor: BackendJobDescriptor): Set[AwsBatchFileOutput] = {
    import cats.syntax.validated._
    def evaluateFiles(output: OutputDefinition): List[WomFile] = {
      Try(
        output.expression.evaluateFiles(jobDescriptor.localInputs, NoIoFunctionSet, output.womType).map(_.toList)
      ).getOrElse(List.empty[WomFile].validNel)
        .getOrElse(List.empty)
    }

    val womFileOutputs = jobDescriptor.taskCall.callable.outputs.flatMap(evaluateFiles) map relativeLocalizationPath

    val outputs: Seq[AwsBatchFileOutput] = womFileOutputs.distinct flatMap {
      _.flattenFiles flatMap {
        case unlistedDirectory: WomUnlistedDirectory => generateUnlistedDirectoryOutputs(unlistedDirectory)
        case singleFile: WomSingleFile => generateAwsBatchSingleFileOutputs(singleFile)
        case globFile: WomGlobFile => generateAwsBatchGlobFileOutputs(globFile)
      }
    }

    val additionalGlobOutput = jobDescriptor.taskCall.callable.additionalGlob.toList.flatMap(generateAwsBatchGlobFileOutputs).toSet

    outputs.toSet ++ additionalGlobOutput
  }

  private def generateUnlistedDirectoryOutputs(womFile: WomUnlistedDirectory): List[AwsBatchFileOutput] = {
    val directoryPath = womFile.value.ensureSlashed
    val directoryListFile = womFile.value.ensureUnslashed + ".list"
    val dirDestinationPath = callRootPath.resolve(directoryPath).pathAsString
    val listDestinationPath = callRootPath.resolve(directoryListFile).pathAsString

    val (_, directoryDisk) = relativePathAndVolume(womFile.value, runtimeAttributes.disks)

    // We need both the collection directory and the collection list:
    List(
      // The collection directory:
      AwsBatchFileOutput(
        makeSafeAwsBatchReferenceName(directoryListFile),
        listDestinationPath,
        DefaultPathBuilder.get(directoryListFile),
        directoryDisk
      ),
      // The collection list file:
      AwsBatchFileOutput(
        makeSafeAwsBatchReferenceName(directoryPath),
        dirDestinationPath,
        DefaultPathBuilder.get(directoryPath + "*"),
        directoryDisk
      )
    )
  }

  private def generateAwsBatchSingleFileOutputs(womFile: WomSingleFile): List[AwsBatchFileOutput] = {
    val destination = callRootPath.resolve(womFile.value.stripPrefix("/")).pathAsString
    val (relpath, disk) = relativePathAndVolume(womFile.value, runtimeAttributes.disks)
    val output = AwsBatchFileOutput(makeSafeAwsBatchReferenceName(womFile.value), destination, relpath, disk)
    List(output)
  }

  private def generateAwsBatchGlobFileOutputs(womFile: WomGlobFile): List[AwsBatchFileOutput] = {
    val globName = GlobFunctions.globName(womFile.value)
    val globDirectory = globName + "/"
    val globListFile = globName + ".list"
    val globDirectoryDestinationPath = callRootPath.resolve(globDirectory).pathAsString
    val globListFileDestinationPath = callRootPath.resolve(globListFile).pathAsString

    val (_, globDirectoryDisk) = relativePathAndVolume(womFile.value, runtimeAttributes.disks)

    // We need both the glob directory and the glob list:
    List(
      // The glob directory:
      AwsBatchFileOutput(makeSafeAwsBatchReferenceName(globDirectory), globDirectoryDestinationPath, DefaultPathBuilder.get(globDirectory + "*"), globDirectoryDisk),
      // The glob list file:
      AwsBatchFileOutput(makeSafeAwsBatchReferenceName(globListFile), globListFileDestinationPath, DefaultPathBuilder.get(globListFile), globDirectoryDisk)
    )
  }

  lazy val monitoringParamName: String = AwsBatchJobPaths.AwsBatchMonitoringKey
  lazy val localMonitoringLogPath: Path = DefaultPathBuilder.get(callPaths.monitoringLogFilename)
  lazy val localMonitoringScriptPath: Path = DefaultPathBuilder.get(callPaths.monitoringScriptFilename)

  lazy val monitoringScript: Option[AwsBatchInput] = {
    callPaths.workflowPaths.monitoringScriptPath map { path =>
      AwsBatchFileInput(s"$monitoringParamName-in", path.pathAsString, localMonitoringScriptPath, workingDisk)
    }
  }

  lazy val monitoringOutput: Option[AwsBatchFileOutput] = monitoringScript map { _ =>
    AwsBatchFileOutput(s"$monitoringParamName-out",
      callPaths.monitoringLogPath.pathAsString, localMonitoringLogPath, workingDisk)
  }

  override lazy val commandDirectory: Path = AwsBatchWorkingDisk.MountPoint

  private val DockerMonitoringLogPath: Path = AwsBatchWorkingDisk.MountPoint.resolve(callPaths.monitoringLogFilename)
  private val DockerMonitoringScriptPath: Path = AwsBatchWorkingDisk.MountPoint.resolve(callPaths.monitoringScriptFilename)

  override def scriptPreamble: String = {
    if (monitoringOutput.isDefined) {
      s"""|touch $DockerMonitoringLogPath
          |chmod u+x $DockerMonitoringScriptPath
          |$DockerMonitoringScriptPath > $DockerMonitoringLogPath &""".stripMargin
    } else ""
  }

  override def globParentDirectory(womGlobFile: WomGlobFile): Path = {
    val (_, disk) = relativePathAndVolume(womGlobFile.value, runtimeAttributes.disks)
    disk.mountPoint
  }

  override def isTerminal(runStatus: RunStatus): Boolean = {
    runStatus match {
      case _: TerminalRunStatus => true
      case _ => false
    }
  }

  // override def executeAsync(): Future[ExecutionHandle] = createNewJob()

  val futureKvJobKey = KvJobKey(jobDescriptor.key.call.fullyQualifiedName, jobDescriptor.key.index, jobDescriptor.key.attempt + 1)

  // TODO: Is this needed? Others don't seem to need
  // override def recoverAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] = reconnectToExistingJob(jobId)
  //
  // override def reconnectAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] = reconnectToExistingJob(jobId)
  //
  // override def reconnectToAbortAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] = reconnectToExistingJob(jobId, forceAbort = true)

  // TODO: Does this need to be implemented
  // private def createNewJob(): Future[ExecutionHandle] = {
  //   // runPipelineResponse map { runId =>
  //   //   val run = Run(runId, initializationData.genomics)
  //   //   PendingExecutionHandle(jobDescriptor, runId, Option(run), previousStatus = None)
  //   // } recover {
  //   //   case JobAbortedException => AbortedExecutionHandle
  //   // }
  //   new AbortedExecutionHandle
  // }

  // override def pollStatusAsync(handle: AwsBatchPendingExecutionHandle): Future[RunStatus] = super[AwsBatchStatusRequestClient].pollStatus(workflowId, handle.runInfo.get)
  override def pollStatusAsync(handle: AwsBatchPendingExecutionHandle): Future[RunStatus] = {
     val jobId = handle.pendingJob.jobId
       throw new RuntimeException(s"not implemented. Jobid $jobId")
     // val job = {
     //   // TODO: Get status from Batch
     //   throw new RuntimeException(s"not implemented. Jobid $jobId")
     // }
     // Future.fromTry(job)
   }


  override lazy val startMetadataKeyValues: Map[String, Any] = super[AwsBatchJobCachingActorHelper].startMetadataKeyValues

  override def getTerminalMetadata(runStatus: RunStatus): Map[String, Any] = {
    runStatus match {
      case _: TerminalRunStatus => Map()
      case unknown => throw new RuntimeException(s"Attempt to get terminal metadata from non terminal status: $unknown")
    }
  }

  override def mapOutputWomFile(womFile: WomFile): WomFile = {
    womFileToPath(generateAwsBatchOutputs(jobDescriptor))(womFile)
  }

  private[aws] def womFileToPath(outputs: Set[AwsBatchFileOutput])(womFile: WomFile): WomFile = {
    womFile mapFile { path =>
      outputs collectFirst {
        case output if output.name == makeSafeAwsBatchReferenceName(path) => output.s3key
      } getOrElse path
    }
  }

  override def isSuccess(runStatus: RunStatus): Boolean = {
    runStatus match {
      case _: RunStatus.Success => true
      case _: RunStatus.UnsuccessfulRunStatus => false
      case _ => throw new RuntimeException(s"Cromwell programmer blunder: isSuccess was called on an incomplete RunStatus ($runStatus).")
    }
  }

  override def getTerminalEvents(runStatus: RunStatus): Seq[ExecutionEvent] = {
    runStatus match {
      case successStatus: RunStatus.Success => successStatus.eventList
      case unknown =>
        throw new RuntimeException(s"handleExecutionSuccess not called with RunStatus.Success. Instead got $unknown")
    }
  }

  override def retryEvaluateOutputs(exception: Exception): Boolean = {
    exception match {
      case aggregated: CromwellAggregatedException =>
        aggregated.throwables.collectFirst { case s: SocketTimeoutException => s }.isDefined
      case _ => false
    }
  }

  override def handleExecutionFailure(runStatus: RunStatus,
                                      handle: StandardAsyncPendingExecutionHandle,
                                      returnCode: Option[Int]): Future[ExecutionHandle] = {
    runStatus match {
      case _: RunStatus.Cancelled => Future.successful(AbortedExecutionHandle)
      case _: RunStatus.UnsuccessfulRunStatus => Future.successful(AbortedExecutionHandle)
      case unknown => throw new RuntimeException(s"handleExecutionFailure not called with RunStatus.Failed. Instead got $unknown")
    }
  }

  override def mapCommandLineWomFile(womFile: WomFile): WomFile = {
    womFile.mapFile(value =>
      getPath(value) match {
        case Success(path: S3Path) => workingDisk.mountPoint.resolve(path.pathWithoutScheme).pathAsString
        case _ => value
      }
    )
  }
}
