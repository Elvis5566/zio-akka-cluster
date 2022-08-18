package zio.akka.cluster.singleton

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{
  ClusterSingletonManager,
  ClusterSingletonManagerSettings,
  ClusterSingletonProxy,
  ClusterSingletonProxySettings
}
//import akka.pattern.ask
import akka.util.Timeout
import zio.{Ref, Runtime, Tag, Task, Unsafe, ZIO, ZLayer}

import scala.concurrent.duration.DurationInt


object SingletonActor {

  def actorName: String = this.getClass.getSimpleName.stripSuffix("$")
  trait Entity[State] {
    def context: ActorContext
    def id: String
    def state: Ref[State]
  }

  trait Proxy[M] {
    def send(message: M): Task[Unit]
    def ask_(message: ActorRef => M)(implicit timeout: Timeout): Task[M]
  }

  def start[R, M, State: Tag](
                               name: String,
                               onMessage: M => ZIO[Entity[State] with R, Throwable, Unit],
                               initState: State,
                               preStartZIO: ZIO[Entity[State] with R, Throwable, Unit] = ZIO.unit
                             ): ZIO[ActorSystem with R, Throwable, Unit] = {
    for {
      rts    <- ZIO.runtime[ActorSystem with R]
      system <- ZIO.service[ActorSystem]
      _ <- ZIO.attempt {
        system.actorOf(
          ClusterSingletonManager.props(
            singletonProps = Props(
              new SingletonEntity[R, M, State](rts, initState)(
                onMessage(_),
                preStartZIO
              )
            ),
            terminationMessage = PoisonPill,
            ClusterSingletonManagerSettings(system)
          ),
          name
        )
      }
    } yield ()

  }

  def proxy[M](name: String): ZIO[ActorSystem, Throwable, Proxy[M]] = {
    for {
      system <- ZIO.service[ActorSystem]
      actor <- ZIO.attempt(
        system.actorOf(
          ClusterSingletonProxy.props(
            singletonManagerPath = s"/user/$name",
            settings = ClusterSingletonProxySettings(system)
          ),
          s"${name}Proxy"
        )
      )
    } yield new Proxy[M] {
      override def send(message: M): Task[Unit] = ZIO.succeed(actor ! message)

      override def ask_(message: ActorRef => M)(implicit timeout: Timeout): Task[M] = {
        import akka.actor.typed.scaladsl.AskPattern._
        import akka.actor.typed.scaladsl.adapter._
        implicit val scheduler = system.toTyped.scheduler

        ZIO.fromFuture { _ => actor.toTyped.ask[M] { ref => message(ref.toClassic) } }
      }
    }

  }

  private[singleton] class SingletonEntity[R, Msg, State: Tag](rts: Runtime[R], initState: State)(
    onMessage: Msg => ZIO[Entity[State] with R, Throwable, Unit],
    preStartZIO: ZIO[Entity[State] with R, Throwable, Unit]
  ) extends Actor {

    val ref: Ref[State] =
      Unsafe.unsafeCompat { implicit u =>
        rts.unsafe.run(Ref.make[State](initState)).getOrThrow()
      }
    val actorContext: ActorContext = context
    val service: Entity[State] = new Entity[State] {
      override def context: ActorContext = actorContext
      override def id: String            = actorContext.self.path.name
      override def state: Ref[State]     = ref
    }

    val entity: ZLayer[Any, Nothing, Entity[State]] = ZLayer.succeed(service)

    override def preStart(): Unit = {
      Unsafe.unsafeCompat { implicit u =>
        rts.unsafe.run(preStartZIO.provideSomeLayer[R](entity)).getOrThrow()
      }
      ()
    }

    def receive: Receive = {
      case msg =>
        Unsafe.unsafeCompat { implicit u =>
          rts.unsafe.run(onMessage(msg.asInstanceOf[Msg]).provideSomeLayer[R](entity)).getOrThrow()
        }
        ()
    }
  }
}