/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.utils

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment

object DemoStreamEnvironment {

  def env: StreamExecutionEnvironment = {
    val config = new Configuration()
    // start the web dashboard
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    // required to start the web dashboard
    config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, "./data/dummyLogFile.txt")

    // create a local stream execution environment
    new LocalStreamEnvironment(config)
  }

}
