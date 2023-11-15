package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class HomePage {
    private WebDriver driver;

    private By formAuthenticationLocator = By.linkText("Form Authentication");
    private By dropDownLocator = By.linkText("Dropdown");
    private By hoversLocator = By.linkText("Hovers");
    private By keyPressesLocator = By.linkText("Key Presses");
    private By alertLocator = By.linkText("JavaScript Alerts");

    public HomePage(WebDriver driver) {
        this.driver = driver;
    }

    public LoginPage clickFormAuthentication() {
        driver.findElement(formAuthenticationLocator).click();
        return new LoginPage(driver);
    }

    public DropDownPage clickDropDown() {
        driver.findElement(dropDownLocator).click();
        return new DropDownPage(driver);
    }

    public HoversPage clickHover() {
        driver.findElement(hoversLocator).click();
        return new HoversPage(driver);
    }

    public KeyPressesPage clickKeyPresses() {
        driver.findElement(keyPressesLocator).click();
        return new KeyPressesPage(driver);
    }

    public AlertsPage clickJavaScriptAlerts() {
        driver.findElement(alertLocator).click();
        return new AlertsPage(driver);
    }
}
