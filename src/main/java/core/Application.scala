package core

/**
  * Created by lzz on 17/5/1.
  */
object Application extends App{
  val mixQuery = new MixQuery
  val queryParam: QueryParam = new QueryParam
  val df = mixQuery.startJob(queryParam)
  df.show()
}
