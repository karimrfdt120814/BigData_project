import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import java.util.Properties
import javax.mail._
import javax.mail.internet._

object SendDataFrameEmail {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Send DataFrame Email Example")
      .master("local[*]") // Change to your cluster settings
      .getOrCreate()

    // Sample DataFrame (replace with your actual DataFrame)
    val data = Seq(
      (1, "Alice", 29),
      (2, "Bob", 31),
      (3, "Cathy", 25)
    )
    val df: DataFrame = spark.createDataFrame(data).toDF("id", "name", "age")

    // Save DataFrame to a temporary CSV file
    val tempFilePath = "C:\\Users\\Asus\\Desktop\\senMail122" // Change this to your desired path
    df.repartition(1).write.option("header", "true").csv(tempFilePath)

    // Send the email with the CSV attachment
    sendEmailWithAttachment(tempFilePath)

    // Stop Spark session
    spark.stop()
  }

  def sendEmailWithAttachment(filePath: String): Unit = {
    // Email configuration
    val host = "smtp.gmail.com" // Gmail SMTP host
    val port = "587" // or "465" for SSL
    val username = "shaikgousyabegam786@gmail.com" // Your Gmail address
    val password = "gousya18" // Your password or App Password

    // Set up the properties for the mail session
    val props = new Properties()
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")
    props.put("mail.smtp.host", host)
    props.put("mail.smtp.port", port)

    // Create a new session with an authenticator
    val session = Session.getInstance(props, new Authenticator() {
      override def getPasswordAuthentication: PasswordAuthentication =
        new PasswordAuthentication(username, password)
    })

    try {
      // Create a new email message
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(username))

      // Set recipients
      val recipientEmail = "recipient@example.com" // Change to recipient's email
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipientEmail).asInstanceOf[Array[Address]])

      message.setSubject("DataFrame CSV Attachment")

      // Create the message body part
      val messageBodyPart = new MimeBodyPart()
      messageBodyPart.setText("Please find attached the DataFrame in CSV format.")

      // Create the attachment part
      val attachmentPart = new MimeBodyPart()
      attachmentPart.attachFile(new File(filePath))

      // Create a multipart message
      val multipart = new MimeMultipart()
      multipart.addBodyPart(messageBodyPart)
      multipart.addBodyPart(attachmentPart)

      // Set the multipart message as the email's content
      message.setContent(multipart)

      // Send the email
      Transport.send(message)

      println("Email sent successfully with attachment!")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
