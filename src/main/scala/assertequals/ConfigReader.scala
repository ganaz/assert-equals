package assertequals

import com.typesafe.config.{Config, ConfigFactory}



object ConfigReader{

  val applicationConf: Config = ConfigFactory.load("application.conf")

}