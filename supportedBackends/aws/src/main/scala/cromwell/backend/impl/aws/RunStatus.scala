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

import cromwell.core.ExecutionEvent
import _root_.io.grpc.Status
import scala.util.Try

sealed trait RunStatus {
  import RunStatus._

  // Could be defined as false for Initializing and true otherwise, but this is more defensive.
  def isRunningOrComplete = this match {
    case Running | _: TerminalRunStatus => true
    case _ => false
  }
}

object RunStatus {
  case object Initializing extends RunStatus
  case object Running extends RunStatus

  sealed trait TerminalRunStatus extends RunStatus {
    def eventList: Seq[ExecutionEvent]
  }

  sealed trait UnsuccessfulRunStatus extends TerminalRunStatus {
    val errorMessage: Option[String]
    lazy val prettyPrintedError: String = errorMessage map { e => s" Message: $e" } getOrElse ""
    val errorCode: Status
  }

  case class Success(eventList: Seq[ExecutionEvent]) extends TerminalRunStatus {
    override def toString = "Success"
  }

  object UnsuccessfulRunStatus {
    def apply(jobId: String, status: String, errorCode: Status, errorMessage: Option[String], eventList: Seq[ExecutionEvent]): UnsuccessfulRunStatus = {
      if (status == "Stopped") {
        Stopped(jobId, errorCode, errorMessage, eventList)
      } else {
        Failed(jobId, errorCode, errorMessage, eventList)
      }
    }
  }
  final case class Stopped(jobId: String,
                          errorCode: Status,
                          errorMessage: Option[String],
                          eventList: Seq[ExecutionEvent],
                          ) extends UnsuccessfulRunStatus {
    override def toString = "Stopped"
  }

  final case class Failed(jobId: String,
                          errorCode: Status,
                          errorMessage: Option[String],
                          eventList: Seq[ExecutionEvent],
                          ) extends UnsuccessfulRunStatus {
    override def toString = "Failed"
  }

  final case class Cancelled(jobId: String,
                          errorCode: Status,
                          errorMessage: Option[String],
                          eventList: Seq[ExecutionEvent],
                          ) extends UnsuccessfulRunStatus {
    override def toString = "Cancelled"
  }
}
