import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class MyStepdefs {

    WebDriver driver;
    @Before
    public void setUp() {
        System.setProperty("webdriver.chrome.driver", "resources/chromedriver");
        driver = new ChromeDriver();
    }

    @Given("I am in the login page of the Para Bank Application")
    public void iAmInTheLoginPageOfTheParaBankApplication() {
        driver.get("https://parabank.parasoft.com/parabank/index.htm");
    }

    @When("I entered valid credentials")
    public void iEnteredValidCredentials() {
    }

    @Then("I should be taken to the overview page")
    public void iShouldBeTakenToTheOverviewPage() {
    }

    @After
    public void tearDown() {
        driver.quit();
    }
}
