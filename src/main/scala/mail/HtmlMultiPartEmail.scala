package mail

import javax.mail.internet.MimeBodyPart
import org.apache.commons.mail.{EmailConstants, MultiPartEmail}

/**
  * Created by WangSiyu on 10/04/2017.
  */
class HtmlMultiPartEmail extends MultiPartEmail {
  def setHtml(html: String): Unit = {
    val rootContainer = this.getContainer
    val msgHtml = new MimeBodyPart
    rootContainer.addBodyPart(msgHtml, 0)

    // EMAIL-104: call explicitly setText to use default mime charset
    //            (property "mail.mime.charset") in case none has been set
    msgHtml.setText(html, this.charset, EmailConstants.TEXT_SUBTYPE_HTML)

    // EMAIL-147: work-around for buggy JavaMail implementations;
    //            in case setText(...) does not set the correct content type,
    //            use the setContent() method instead.
    val contentType = msgHtml.getContentType
    if (contentType == null || !(contentType == EmailConstants.TEXT_HTML)) { // apply default charset if one has been set
      if (this.charset != null && this.charset.length > 0) {
        msgHtml.setContent(html, EmailConstants.TEXT_HTML + "; charset=" + this.charset)
      } else {
        // unfortunately, MimeUtility.getDefaultMIMECharset() is package private
        // and thus can not be used to set the default system charset in case
        // no charset has been provided by the user
        msgHtml.setContent(html, EmailConstants.TEXT_HTML)
      }
    }
  }
}
