package alert;

import base.BaseTests;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class AlertTests extends BaseTests {
    @Test
    public void testAcceptAlert() {
        var javaScriptAlerts = homePage.clickJavaScriptAlerts();
        javaScriptAlerts.triggerAlert();
        javaScriptAlerts.alertClickToAccept();
        assertEquals(javaScriptAlerts.getResult(), "You successfully clicked an alert", "Result text incorrect!");
    }

    @Test
    public void testGetTextFromAlert() {
        var javaScriptAlerts = homePage.clickJavaScriptAlerts();
        javaScriptAlerts.triggerConfirm();
        String text = javaScriptAlerts.alertGetText();
        javaScriptAlerts.alertClickToDismiss();
        assertEquals(text, "I am a JS Confirm", "Alert text incorrect");
    }
}
