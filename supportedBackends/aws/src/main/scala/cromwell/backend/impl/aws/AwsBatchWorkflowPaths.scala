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

import com.typesafe.config.Config
import software.amazon.awssdk.core.auth.{AwsCredentials}
import akka.actor.ActorSystem
import cromwell.backend.io.WorkflowPaths
import cromwell.backend.{BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.core.path.{PathBuilder, PathFactory}
import net.ceedubs.ficus.Ficus._

object AwsBatchWorkflowPaths {
}

case class AwsBatchWorkflowPaths(workflowDescriptor: BackendWorkflowDescriptor,
                            credentials: AwsCredentials,
                            configuration: AwsBatchConfiguration)(implicit actorSystem: ActorSystem) extends WorkflowPaths {

  override def config: Config = configuration.configurationDescriptor.backendConfig
  override val pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders

  val DockerRootString = config.as[Option[String]]("dockerRoot").getOrElse("/cromwell-executions")
  var DockerRoot = PathFactory.buildPath(DockerRootString, pathBuilders)
  if (!DockerRoot.isAbsolute) {
    DockerRoot = PathFactory.buildPath("/".concat(DockerRootString), pathBuilders)
  }
  val dockerWorkflowRoot = workflowPathBuilder(DockerRoot)

  override def toJobPaths(workflowPaths: WorkflowPaths,
                          jobKey: BackendJobDescriptorKey): AwsBatchJobPaths = {
    new AwsBatchJobPaths(workflowPaths.asInstanceOf[AwsBatchWorkflowPaths], jobKey)
  }

  override protected def withDescriptor(workflowDescriptor: BackendWorkflowDescriptor): WorkflowPaths = this.copy(workflowDescriptor = workflowDescriptor)
}
