import static org.junit.Assert.*;
import org.junit.Test;

public class URLFilterTest {
    @Test
    public void myTest(){
        URLFilter filter = new URLFilter(
                "Disallow: /massages\n" +
                "Disallow: /kiev\n" +
                "Disallow: */reviews.html$\n" +
                "Disallow: /dnepropetrovsk\n" +
                "Disallow: /krivoy-rog\n" +
                "Disallow: */vacancy.html$"
        );
        assertFalse(filter.IsAllowed("/dnepropetrovsk/"));
    }

    @Test
    public void testSimpleCase()  {
        URLFilter filter = new URLFilter("Disallow: /users");

        assertTrue(filter.IsAllowed("/company/about.html"));

        assertFalse(filter.IsAllowed("/users/jan"));
        assertFalse(filter.IsAllowed("/users/"));
        assertFalse(filter.IsAllowed("/users"));

        assertTrue("should be allowed since in the middle", filter.IsAllowed("/another/prefix/users/about.html"));
        assertTrue("should be allowed since at the end", filter.IsAllowed("/another/prefix/users"));
    }

    @Test
    public void testEmptyCase() {
        URLFilter filter = new URLFilter();

        assertTrue(filter.IsAllowed("/company/about.html"));
        assertTrue(filter.IsAllowed("/company/second.html"));
        assertTrue(filter.IsAllowed("any_url"));
    }

    @Test
    public void testEmptyStringCase() {
        // that's different from testEmptyCase() since we
        // explicitly pass empty robots_txt rules
        URLFilter filter = new URLFilter("");

        assertTrue(filter.IsAllowed("/company/about.html"));
        assertTrue(filter.IsAllowed("/company/second.html"));
        assertTrue(filter.IsAllowed("any_url"));
    }

    @Test
    public void testRuleEscaping()  {
        // we have to escape special characters in rules (like ".")
        URLFilter filter = new URLFilter("Disallow: *.php$");

        assertFalse(filter.IsAllowed("file.php"));
        assertTrue("sphp != .php", filter.IsAllowed("file.sphp"));
    }

    @Test
    public void testAllCases()  {
        String rules = "Disallow: /users\n" +
                "Disallow: *.php$\n" +
                "Disallow: */cgi-bin/\n" +
                "Disallow: /very/secret.page.html$\n";

        URLFilter filter = new URLFilter(rules);

        assertFalse(filter.IsAllowed("/users/jan"));
        assertTrue("should be allowed since in the middle", filter.IsAllowed("/subdir2/users/about.html"));

        assertFalse(filter.IsAllowed("/info.php"));
        assertTrue("we disallowed only the endler", filter.IsAllowed("/info.php?user=123"));
        assertTrue(filter.IsAllowed("/info.pl"));

        assertFalse(filter.IsAllowed("/forum/cgi-bin/send?user=123"));
        assertFalse(filter.IsAllowed("/forum/cgi-bin/"));
        assertFalse(filter.IsAllowed("/cgi-bin/"));
        assertTrue(filter.IsAllowed("/scgi-bin/"));


        assertFalse(filter.IsAllowed("/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/the/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/very/secret.page.html?blah"));
        assertTrue("we disallowed only the whole match", filter.IsAllowed("/the/very/secret.page.html?blah"));
    }
}