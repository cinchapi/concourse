/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.QuoteAwareStringSplitter;
import com.cinchapi.concourse.util.Strings;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link QuoteAwareStringSplitter class}.
 * 
 * @author Jeff Nelson
 */
public class QuoteAwareStringSplitterTest {

    /**
     * Test logic for ensuring that the quote aware string splitter works.
     * 
     * @param string
     * @param delim
     */
    private void doTestSplitWithQuotes(String string, char delim) {
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string,
                delim);
        String[] toks = Strings.splitStringByDelimiterButRespectQuotes(string,
                String.valueOf(delim));
        int i = 0;
        while (it.hasNext()) {
            String tok = toks[i];
            if(!tok.isEmpty()) {
                String next = it.next();
                if(next.startsWith("'") && next.endsWith("'")) {
                    // Strings#splitStringByDelimiterButRespectQuotes replaces
                    // single quotes with double quotes, so we must do that here
                    // in order to do the comparison
                    next = "\"" + next.substring(1, next.length() - 1) + "\"";
                }
                Assert.assertEquals(tok, next);
            }
            ++i;
        }
    }

    @Test
    public void testSplitWithSingleQuotes() {
        doTestSplitWithQuotes(
                "this string is going to be split by 'space but we are respecting' single quotes",
                ' ');
    }

    @Test
    public void testSplitWithDoubleQuotes() {
        doTestSplitWithQuotes(
                "this string is going to be split by \"space but we are respecting\" double quotes",
                ' ');
    }

    @Test
    public void testDontSplitOnApostrophe() {
        doTestSplitWithQuotes(
                "don't split the string on the apostrophe 'because it needs to work'",
                ' ');
    }

    @Test
    public void testSplitOnNewlineLF() {
        String string = "this\nis a 'quote across\nlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\nlines'", it.next());
    }

    @Test
    public void testSplitOnNewlineCR() {
        String string = "this\nis a 'quote across\rlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\rlines'", it.next());
    }

    @Test
    public void testSplitOnNewlineCRLF() {
        String string = "this\nis a 'quote across\r\nlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\r\nlines'", it.next());
    }

    @Test
    public void testTokenizeParenthesis() {
        String string = "foo(bar) \"but don't (split this)\" but split ( this)";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.TOKENIZE_PARENTHESIS);
        while (it.hasNext()) {
            Assert.assertEquals("foo", it.next());
            Assert.assertEquals("(", it.next());
            Assert.assertEquals("bar", it.next());
            Assert.assertEquals(")", it.next());
            Assert.assertEquals("\"but don't (split this)\"", it.next());
            Assert.assertEquals("but", it.next());
            Assert.assertEquals("split", it.next());
            Assert.assertEquals("(", it.next());
            Assert.assertEquals("this", it.next());
            Assert.assertEquals(")", it.next());
        }
    }

    @Test
    public void testDropQuotes() {
        String string = "a,b,\"c,d,efg,h\"";
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string, ',',
                SplitOption.DROP_QUOTES);
        while (it.hasNext()) {
            Assert.assertFalse(Strings.isWithinQuotes(it.next()));
        }
    }

    @Test
    public void testHandleEscapedQuotes() {
        String string = "103403,theme_mods_simplemag,\"a:63:{i:0;b:0;s:7:\\\"backups\\\";N;s:9:\\\"smof_init\\\";s:31:\\\"Mon, 26 May 2014 23:29:41 +0000\\\";s:9:\\\"site_logo\\\";s:58:\\\"[site_url]/wp-content/uploads/2014/04/Blavitylogoblack.png\\\";s:12:\\\"site_tagline\\\";s:1:\\\"0\\\";s:12:\\\"site_favicon\\\";s:49:\\\"[site_url]/wp-content/uploads/2014/05/favicon.ico\\\";s:19:\\\"site_retina_favicon\\\";s:0:\\\"\\\";s:14:\\\"site_top_strip\\\";i:1;s:22:\\\"site_sidebar_behaviour\\\";s:1:\\\"1\\\";s:24:\\\"site_wide_excerpt_length\\\";s:2:\\\"24\\\";s:17:\\\"site_wide_excerpt\\\";i:1;s:18:\\\"site_page_comments\\\";i:0;s:16:\\\"site_author_name\\\";i:0;s:14:\\\"copyright_text\\\";s:22:\\\"Copyright Blavity 2014\\\";s:15:\\\"main_site_color\\\";s:7:\\\"#ffffff\\\";s:20:\\\"site_top_strip_color\\\";s:16:\\\"color-site-white\\\";s:15:\\\"main_menu_links\\\";s:4:\\\"14px\\\";s:11:\\\"slider-tint\\\";s:16:\\\"slider-tint-dark\\\";s:20:\\\"slider_tint_strength\\\";s:3:\\\"0.1\\\";s:26:\\\"slider_tint_strength_hover\\\";s:3:\\\"0.7\\\";s:17:\\\"site_footer_color\\\";s:15:\\\"color-site-gray\\\";s:11:\\\"font_titles\\\";s:5:\\\"Bayon\\\";s:9:\\\"font_text\\\";s:4:\\\"Lato\\\";s:21:\\\"single_media_position\\\";s:9:\\\"fullwidth\\\";s:21:\\\"single_featured_image\\\";i:1;s:18:\\\"single_author_name\\\";i:0;s:17:\\\"single_wp_gallery\\\";i:1;s:13:\\\"single_social\\\";i:1;s:13:\\\"single_author\\\";i:1;s:17:\\\"single_nav_arrows\\\";i:1;s:10:\\\"post_score\\\";s:38:\\\"<h3 style=\\\"margin: 0;\\\">Post Score</h3>\\\";s:19:\\\"single_rating_title\\\";s:10:\\\"Our Rating\\\";s:22:\\\"single_breakdown_title\\\";s:13:\\\"The Breakdown\\\";s:18:\\\"related_posts_info\\\";s:41:\\\"<h3 style=\\\"margin: 0;\\\">Related Posts</h3>\\\";s:14:\\\"single_related\\\";s:1:\\\"0\\\";s:20:\\\"single_related_title\\\";s:17:\\\"You may also like\\\";s:28:\\\"single_related_posts_to_show\\\";s:1:\\\"2\\\";s:15:\\\"slide_dock_info\\\";s:51:\\\"<h3 style=\\\"margin: 0;\\\">Random Posts Slide Dock</h3>\\\";s:17:\\\"single_slide_dock\\\";i:1;s:23:\\\"single_slide_dock_title\\\";s:12:\\\"More Stories\\\";s:23:\\\"single_slide_dock_style\\\";i:0;s:19:\\\"top_social_profiles\\\";s:1:\\\"1\\\";s:7:\\\"sp_feed\\\";s:0:\\\"\\\";s:11:\\\"sp_facebook\\\";s:24:\\\"www.facebook.com/Blavity\\\";s:10:\\\"sp_twitter\\\";s:23:\\\"www.Twitter.com/Blavity\\\";s:9:\\\"sp_google\\\";s:36:\\\"https://plus.google.com/+BlavityPage\\\";s:11:\\\"sp_linkedin\\\";s:0:\\\"\\\";s:12:\\\"sp_instagram\\\";s:28:\\\"http://instagram.com/blavity\\\";s:9:\\\"sp_flickr\\\";s:0:\\\"\\\";s:8:\\\"sp_vimeo\\\";s:0:\\\"\\\";s:10:\\\"sp_youtube\\\";s:0:\\\"\\\";s:10:\\\"sp_behance\\\";s:0:\\\"\\\";s:10:\\\"sp_dribble\\\";s:0:\\\"\\\";s:12:\\\"sp_pinterest\\\";s:0:\\\"\\\";s:13:\\\"sp_soundcloud\\\";s:0:\\\"\\\";s:9:\\\"sp_lastfm\\\";s:0:\\\"\\\";s:10:\\\"custom_css\\\";s:35:\\\".single .message { display: none; }\\\";s:16:\\\"custom_js_header\\\";s:0:\\\"\\\";s:16:\\\"custom_js_footer\\\";s:0:\\\"\\\";s:11:\\\"error_title\\\";s:56:\\\"Omph. Our bad. We're rolling our eyes too! Try again. :)\\\";s:11:\\\"error_image\\\";s:60:\\\"[site_url]/wp-content/uploads/2014/05/hangout_snapshot_2.jpg\\\";s:18:\\\"nav_menu_locations\\\";a:2:{s:9:\\\"main_menu\\\";i:2;s:14:\\\"secondary_menu\\\";i:26;}s:16:\\\"sidebars_widgets\\\";a:2:{s:4:\\\"time\\\";i:1407380058;s:4:\\\"data\\\";a:6:{s:19:\\\"wp_inactive_widgets\\\";a:1:{i:0;s:6:\\\"text-2\\\";}s:9:\\\"sidebar-1\\\";a:4:{i:0;s:14:\\\"mc4wp_widget-4\\\";i:1;s:17:\\\"ti_image_banner-2\\\";i:2;s:18:\\\"facebook-likebox-2\\\";i:3;s:14:\\\"recent-posts-2\\\";}s:9:\\\"sidebar-2\\\";a:3:{i:0;s:16:\\\"ti_video_embed-2\\\";i:1;s:17:\\\"ti_latest_posts-2\\\";i:2;s:10:\\\"nav_menu-3\\\";}s:9:\\\"sidebar-3\\\";a:1:{i:0;s:10:\\\"nav_menu-2\\\";}s:9:\\\"sidebar-4\\\";a:1:{i:0;s:14:\\\"mc4wp_widget-5\\\";}s:9:\\\"sidebar-5\\\";a:0:{}}}}\",yes,wp_options";
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string, ',');
        int count = 0;
        while (it.hasNext()) {
            it.next();
            ++count;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void testHandleLeadingApostrophe() {
        String string = "12 great theme songs every '90s black kid will remember,,inherit,closed,closed,,14321-revision-v1,,,2015-08-17 16:01:05,2015-08-17 20:01:05,,14321,http://staging.blavity.com/14321-revision-v1/,0,revision,,0,wp_posts\n17114,616166,2015-10-07 22:1Exception in thread";
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string, ',');
        String[] toks = string.split(",");
        int count = 0;
        while (it.hasNext()) {
            Assert.assertEquals(toks[count], it.next());
            ++count;
        }
    }

    @Test
    public void testHandleLeadingApostropeAndQuotes() {
        String string = "'90s kids are awesome,a,b,\"here,is,a,quote\",c,d,\"and,another,quote\"";
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string, ',');
        int count = 0;
        while (it.hasNext()) {
            it.next();
            ++count;
        }
        Assert.assertEquals(7, count);
    }

    @Test
    public void testQuoteAwareTrim() {
        String string = "a, b, \"c, d, e\", f  ,g";
        StringSplitter it = new QuoteAwareStringSplitter(string, ',',
                SplitOption.TRIM_WHITESPACE);
        List<String> expected = Lists.newArrayList("a", "b", "\"c, d, e\"",
                "f", "g");
        int index = 0;
        while (it.hasNext()) {
            String next = it.next();
            if(!next.contains("\"")) {
                Assert.assertFalse(next.contains(" "));
            }
            Assert.assertEquals(expected.get(index), next);
            ++index;
        }

    }

}
