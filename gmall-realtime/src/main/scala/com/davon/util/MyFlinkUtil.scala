package com.davon.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2021/07/28 22:04
 */
object MyFlinkUtil {
  def setCKProperties(env: StreamExecutionEnvironment, ckInterval: Long, ckTimeOut: Long) = {
    env.enableCheckpointing(ckInterval)
    env.getCheckpointConfig.setCheckpointTimeout(ckTimeOut)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink1109/ck"))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L))
  }
}
