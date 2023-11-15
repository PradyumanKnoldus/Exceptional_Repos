package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class AlertsPage {
    private WebDriver driver;
    private By triggerAlertButtonLocator = By.xpath("//button[text() = 'Click for JS Alert']");
    private By triggerConfirmButtonLocator = By.xpath("//button[text() = 'Click for JS Confirm']");
    private By resultsLocator = By.id("result");

    public AlertsPage(WebDriver driver) {
        this.driver = driver;
    }

    public void triggerAlert() {
        driver.findElement(triggerAlertButtonLocator).click();
    }

    public void triggerConfirm() {
        driver.findElement(triggerConfirmButtonLocator).click();
    }

    public void alertClickToAccept() {
        driver.switchTo().alert().accept();
    }

    public void alertClickToDismiss() {
        driver.switchTo().alert().dismiss();
    }

    public String alertGetText() {
        return driver.switchTo().alert().getText();
    }

    public String getResult() {
        return driver.findElement(resultsLocator).getText();
    }
}
