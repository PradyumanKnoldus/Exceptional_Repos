//import scalafx.application.JFXApp
//import scalafx.application.JFXApp.PrimaryStage
//import scalafx.scene.Scene
//import scalafx.scene.control.{Button, Label, TextField}
//import scalafx.event.ActionEvent
//
//import scalaj.http.{Http, HttpResponse}
//
//object RestApiApp extends JFXApp {
//  override def start(primaryStage: PrimaryStage): Unit = {
//    val urlLabel = new Label("API URL:")
//    urlLabel.layoutX = 20
//    urlLabel.layoutY = 20
//
//    val urlField = new TextField()
//    urlField.layoutX = 100
//    urlField.layoutY = 20
//    urlField.prefWidth = 250
//
//    val sendButton = new Button("Send Request")
//    sendButton.layoutX = 150
//    sendButton.layoutY = 60
//    sendButton.onAction = (_: ActionEvent) => {
//      val url = urlField.text.value
//      val response: HttpResponse[String] = Http(url).asString
//      // Update the GUI with the response
//      println(response.body)
//    }
//
//    val scene = new Scene(400, 200) {
//      content = List(urlLabel, urlField, sendButton)
//    }
//
//    primaryStage.title = "REST API App"
//    primaryStage.scene = scene
//    primaryStage.show()
//  }
//
//  def main(args: Array[String]): Unit = {
//    launch()
//  }
//}
