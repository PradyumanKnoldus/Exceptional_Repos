package hover;

import base.BaseTests;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class HoverTests extends BaseTests {
    @Test
    public void testHoverUser1() {
        var hoversPage = homePage.clickHover();
        var caption = hoversPage.hoverOverFigure(1);
        assertTrue(caption.isCaptionDisplayed(), "Caption Not Displayed");
        assertEquals(caption.getTitle(), "name: user1", "Caption Title Incorrect!");
        assertEquals(caption.getLinkText(), "View profile", "Caption Link Text Incorrect!");
        assertTrue(caption.getLink().endsWith("/users/1"), "Link Incorrect");
    }
}
