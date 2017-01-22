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
package com.cinchapi.concourse.server.plugin.io;

import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.google.common.base.Throwables;

/**
 * Unit tests for the {@link InterProcessCommunication} class.
 *
 * @author Jeff Nelson
 */
public abstract class InterProcessCommunicationTest extends ConcourseBaseTest {
    
    @Test
    public void testBasicRead() {
        String location = FileOps.tempFile();
        InterProcessCommunication queue = getInterProcessCommunication(location);
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        InterProcessCommunication queue2 = getInterProcessCommunication(location);
        Assert.assertNotEquals(queue, queue2);
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);
    }
    @Test
    public void testBasicWrite() {
        InterProcessCommunication queue = getInterProcessCommunication();
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);
    }
    

    @Test
    public void testCompactionAcrossInstancesForReads() {
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file);
        InterProcessCommunication sm2 = getInterProcessCommunication(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm1.write(ByteBuffers.fromString("bbbb"));
        sm1.write(ByteBuffers.fromString("ccccc"));
        sm1.read();
        sm2.compact();
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("bbbb"));
        Assert.assertEquals(sm1.read(), ByteBuffers.fromString("ccccc"));

    }

    @Test
    public void testCompactionWhenEmpty() {
        InterProcessCommunication memory = getInterProcessCommunication();
        memory.compact();
        Assert.assertTrue(true); // lack of exception means test passes
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByRead() {
        InterProcessCommunication memory = getInterProcessCommunication();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        Assert.assertEquals(ByteBuffers.fromString("ccccc"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByReadWriteRead() {
        InterProcessCommunication memory = getInterProcessCommunication();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        memory.read();
        memory.write(ByteBuffers.fromString("ff"));
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("ff"), memory.read());
    }

    @Test
    public void testCompactionWithUnreadMessagesFollowedByWriteRead() {
        InterProcessCommunication memory = getInterProcessCommunication();
        memory.write(ByteBuffers.fromString("aaa"));
        memory.write(ByteBuffers.fromString("bbbb"));
        memory.write(ByteBuffers.fromString("ccccc"));
        memory.write(ByteBuffers.fromString("dddddd"));
        memory.write(ByteBuffers.fromString("eeeeeee"));
        memory.read();
        memory.read();
        memory.compact();
        memory.write(ByteBuffers.fromString("ff"));
        Assert.assertEquals(ByteBuffers.fromString("ccccc"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("dddddd"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("eeeeeee"), memory.read());
        Assert.assertEquals(ByteBuffers.fromString("ff"), memory.read());
    }

    @Test
    public void testMultipleConcurrentWriters() {
        InterProcessCommunication memory = getInterProcessCommunication();
        int writers = Random.getScaleCount();
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean passed = new AtomicBoolean(true);
        AtomicInteger ran = new AtomicInteger(0);
        for (int i = 0; i < writers; ++i) {
            executor.execute(() -> {
                try {
                    ByteBuffer data = ByteBuffer.allocate(4);
                    data.putInt(Random.getInt());
                    data.flip();
                    memory.write(data);
                    ran.incrementAndGet();
                }
                catch (OverlappingFileLockException e) {
                    passed.set(false);
                }
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        Assert.assertTrue(passed.get());
        Assert.assertEquals(ran.get(), writers);
    }

    @Test
    public void testReadPeekNewMessageWhenAtBufferCapacity() { // bug repro
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file, 4);
        InterProcessCommunication sm2 = getInterProcessCommunication(file, 4);
        ByteBuffer message = ByteBuffer.allocate(4);
        message.putInt(1);
        message.flip();
        sm1.write(message);
        message = ByteBuffer.allocate(4);
        message.putInt(2);
        message.flip();
        sm1.write(message);
        message = ByteBuffer.allocate(4);
        message.putInt(3);
        message.flip();
        sm1.write(message);
        Assert.assertEquals(1, sm2.read().getInt());
        Assert.assertEquals(2, sm2.read().getInt());
        Assert.assertEquals(3, sm2.read().getInt());
    }

    @Test
    public void testReadWriteNoRaceCondition() throws InterruptedException {
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file);
        InterProcessCommunication sm2 = getInterProcessCommunication(file);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean read = new AtomicBoolean(false);
        Thread t1 = new Thread(() -> {
            sm1.read();
            read.set(true);
            latch.countDown();
        });
        Thread t2 = new Thread(() -> {
            sm2.write(ByteBuffers.fromString("aaa"));
            latch.countDown();
        });
        t2.start();
        t1.start();
        latch.await();
        Assert.assertTrue(read.get());
    }

    @Test
    public void testWriteReadAfterCompactionWhenNoUnreadMessages() {
        String file = FileOps.tempFile();
        InterProcessCommunication sm1 = getInterProcessCommunication(file);
        InterProcessCommunication sm2 = getInterProcessCommunication(file);
        sm1.write(ByteBuffers.fromString("aaa"));
        sm2.read();
        sm1.write(ByteBuffers.fromString("bbb"));
        sm2.read();
        sm1.write(ByteBuffers.fromString("ccc"));
        sm2.read();
        sm2.compact();
        sm1.write(ByteBuffers.fromString("ddd"));
        Assert.assertEquals(ByteBuffers.fromString("ddd"), sm2.read());
    }

    protected abstract InterProcessCommunication getInterProcessCommunication();

    protected abstract InterProcessCommunication getInterProcessCommunication(String file);

    protected abstract InterProcessCommunication getInterProcessCommunication(String file, int capacity);

    

// @formatter:off
//    @Test
//    public void testStreamingWriteAndRead() throws InterruptedException {
//        InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 10;
//        try {
//            String file = FileOps.tempFile();
//            InterProcessCommunication writer = getInterProcessCommunication(file);
//            InterProcessCommunication reader = getInterProcessCommunication(file);
//            String expected = new RandomStringGenerator().nextString(15000);
//            ByteBuffer message = ByteBuffers.fromString(expected);
//            CountDownLatch latch = new CountDownLatch(2);
//            Thread t1 = new Thread(() -> {
//                writer.write(message);
//                latch.countDown();
//            });
//            AtomicReference<String> actual = new AtomicReference<>(null);
//            Thread t2 = new Thread(() -> {
//                actual.set(ByteBuffers.getString(reader.read()));
//                latch.countDown();
//            });
//            t1.start();
//            t2.start();
//            latch.await();
//            Assert.assertEquals(expected, actual.get());
//        }
//        finally {
//            InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 0;
//        }
//    }
//
//    @Test
//    public void testStreamingWriteAndReadReproA() throws InterruptedException {
//        InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 10;
//        try {
//            String file = FileOps.tempFile();
//            InterProcessCommunication writer = getInterProcessCommunication(file);
//            InterProcessCommunication reader = getInterProcessCommunication(file);
//            String expected = "tfjefjhwpglbgbqhewyrobukrolhhinannakmncpyainbulvzkgkgzcvpdfeychcaptdxqgexvlkekshgwkoacedpttdimavgnwzcerepvqeauvhnykvwppetbqavmtklqmlnhsqzhfmdoyrglcydgdmqoaubqdmzyytpexlvokyejhxsssweeaihhkjwfpnbvmmeipfkdbfwhhppeixtvckhmppktjuhrjvwauawvtwemqwtqdyssyrywjxznmhtvxsqquzfuscywzcielqqbljlkbsildrdwwpniiwdbjjkigxzgjfxasdesicmmallekdldquvyttlqoplucccankrcowiotnrsqealtqsldshokvpzlsvrzgtvodibviozraaswqekiijytcrrazvvcwzamoksjxyomfnvprkuznurupfodbkncsucuzcdqxaxpndchoygugyhpggsayodsmxuofpnamkmmxuiqqkmtvqxhuefvshoioxiaeugccibugbcwnlfvriajqvnfrbmsgigxsihuwowhuuptrgdnmpngyvwhvrgsuaxzmkbbpoaqfeutnlxyxveyfidfacpsaxhpsejdpkdhvtybsynnjnzewujxstfszeabosswcqsftpcqyqraqdenchuscouzpmglulrjhrdwmvqrchrautcvgcqbmdaxbebarkpblkgasogryrabalqkrnmekyiknobikcrjrlqkuxjkiyaqyuomyauwbfmtgcvklongsuxuwpjqrjnzzrceubrerkpyophobhpzlywcbbjqqutloztrselorjykohhoxbfuombtdbfsxsctsvfvtzndbkqxskfeeuvajnxsqzhoscetyqowojvbtpplridmggciggswgzlzuqphrifqfblxieslijngpsqeausixlxnpcgojnttggmufzsvghdpwgrvzxnlcboagatybfxlbgypchhbtqleaswarvfmkjkuvjivuuiahhxldlecbofbaovdrtntvwrrjlerkipoozpsstpwubhmonoxbpbtdpzpsrhqjbchvvmhvrxrjnvbhxflugfwhnjlqatenremidslihexqwjllrvajfmfczbppmdencgdvfotuhdhlagavtkfkmazlmzcrnxnxjwnxttjeqchgzxsdwqjypezhinionqfrbjfilzjbapldnufzaghsjfgcdvgnfyjthulzmlqfkfmqpqiwklnmrethkzqtquqthcuzxhrqeirrtllrjdvbidydxmgjadpuzpuyrrdyabrrntwqukxsznqwlfouedihmjxzlcvkbohdhwkbkqwkdhhwcebyixacawyizcfcpluhdqfftsztwzurpdibfmfvueeuuflcejtaarunqgkakjhbtgqguurtycqjulwdfbngqyzbryjuosbndpxfywxnpmxolrjqrkdrgqvoobyntumedbaapttyyzsaihfftyzbskqwopuleymyqxeziyspmioqhcpmfuiwnppfbjoisgkominzbcsabchaqfrnvbrbeepsgduylurfszwsdtcnlkodzdmojnhsiyfdqlchudclxbqiozdrcrrouiybopcfebrbuxfattetbrtmdtdggjouvdhvagjkvxudcqctxykxhuqdyhltvdonvrvbiafavtmddgggbijsbjacmpscobrbydudtwfomtyehtduqbmezdgkbbvqntshsnklbuvesobyryqjnwzgdkdjirroutslkmoghtgduwrqyhpqjckagppfcpdyseevstpuhmoydosoewypwycrhhxouwprvzbswvhlyvwmiznddynpjtnqmqawiniypjmwzpuwwjykoplykbhgawywquddgosdxdeezdnsbaiaqmrodbnztoqfeuhugnfhaswqfiupxbngifzvmpcmjwfmsfguuzlxmvhdvkweucdmsjktnmrtxxjnfyunxvztsllsyilrlonstecknhgjijwwfipynedrsinoadepyhcjoafflzhdvkwjdeygumdlwowadsybmvkjapxnnsvgerqfiyoztkegzpftdisywidrqklipekrbtqndrqnneisquisgnprhusryvjkdtbdzaushqoalifqbpyzlzgxpjblfpcbsfadoilwmgczzgwdhhmjdgtmbcjmfiwagvnfoodsxkhixeryorutsheeyyiieegqwztqizkpsfuvgqfetekdwiimsxuktvinluxucnknuxpmajcyvagyidkbkxclqozhdotqhdhflgqhhrnpmvunzpsexjjxxxsshrmunkvrxxtwdcjexavijqqjyueujaoeqzieijegajdccqasyoycdgzuglqzwmewuusxbtztxphkjnuryavyfhxydeidqaeclueqkskbatutjayvluwpwctskvisxechgcnohyesuguqeruzprjlnxpuchjzgkpzzmbfrzzjysrilmzpiyzfthppzdxadokhmvzwprfarmteapeiiotfeptcpfamuuqgzoltfiqllbskcioppwddaughcnqcxyifllaxckuxorumyqjudocjlvxcpcxbnlnoeivfgbbmxpbrrrldzusyztofyjjwdbtjiehfbftkuqoyhjpliauutjfayvvcxkrfpajswfmzeexeobrtppnkndewxrehksxcoedoubswaywxpsebliamsvrlzaxwjwomaomkwbzgcqyorjfcscpnpwpjacnwyybgiiseyrrdjeqiojnqfwkuqyshqmcujrjywurwbdfhizapndkpbkhxowoakmapglyjaarlpyylgldlktacwhefuabwkcnsiogxxxzhvjbjhriupqukdfjjfxvaahfsomvearjhovcmrilarutolfaukmuefskivzicmriqurkkopfrxkopybmdyttwipepwhspjjydeybftykxkmxygqeoebmiwjaqkzavkkxdmdhlsurwijiatdxcawhdwxekzsjrxitqcohhejmwfsxvvbsdmnwbztanzoraxpczhshtihlcomaxotkihymvtjjcgwzxvvxyvahpwscvglhcbuaeswbewwcicokqkvochuliaexjayacvedqnkgeueznzmwczcsoelzptrzwuvljxkoknnzxzdtkncbiamowwppklmzysyszreeruyvstulnvduyzyhgdwkqfcxdjzmmknojgviqooggthziuommzawrfzddwsnpailoxkhiyfpglliwkvhbqqysppuzemocimfqmhrilgyajdjeqgvjvbrncdpwznojwjnkzqlyxqepdqyopwhxutwlsurbcqiizmaxyenotvytvsnsdohrwufqjhdwpwyvqpjfquwsrlgeivkscavgtozeeptnvrfcqjkduuqoiqmcsmwsxnpsyeklfqguuwckhierfavwvjhzaaphuejtkcdjszngyubyzpiakzxxvejaneysmrfvoossolicsvgopclscctwwuzfrxamogugkhwzaivszqvgsfngddljuiijctuoornlxjlxanzufrxsyntjtdbfqbwxnachulycmukolozaluicpqkhfwfofqnxovuxnskvlchtmybdorzmnapebbvjnxaenwsopgnuixykgnovsjyjfzasoghpwyilpnjluhiszwerkqgsufypefkufcfmdyxzlnlybsuedfdppdgdxrbjdtquzieosxoxieplwjcrpicqrwsqbpfgkygrfxrwgdtgweezuhfstgaggpogbrdyozchujjmpkanspvftmfqnsgrueibyurwleseqvtawxwawirqeucpugwtnzojecxdliukrjhlrpjjpkzacfsjehlddtdeguamtwekqmdgommryfqkfdxcfriktsbxytvncoqvbeacpuxkkfwwllvnvapjqnwwukbafhiztyojkoqdmbbxhhrsyaqqigpxjotzugyfgkqnqkufasousoerkalxksqnjbtncynvcvtlzfwgbohzxasemcpnfvrbhcastyecufweflglaacsgdydhqcxdoogelairkkfpgsigbcpnhrsvnqqhsovbhopixkfjhcdadspolurcdmlqbgajfswtvfonvkadkdeothrprjlwoucfgsufklwwzqokzvkdjzqdeiigweulffswtywygewapppfhjwegriguoohejlayzdtcjsfnomrqrqtmkqeexsjycpvlipjyvxbtgbdoyvseorvmmnoihphzvinwivoiqstbgcappigambytymgkgjuzuxcwyssfqdgtzpjsiydhnnpnvyujnuahnjgojzymtosqokzhgezdsercbnchvpnswyrzrmccrkoekdphmipubbclkrxgfoqehwnyjxejwpocfyhxrqpzdxowtaycyfoobgrimlkebcedawbqmhjokjqhyuhmrmsvmhrqpqlyxhggqxscywtqwmhxctexyvfzbdnbhlqmkatwuvvgnghtnnyydynzgewyqtizzgpwvjntnxlocewlbucrrwhrogswlhrsddnhxldjeuizibrlghawseupinruaarfdxylpjiuypeoujwvjhtefqnkvezuqwhlhqxtktcdfbmtcysgzqkodcsvzopvrzbwqqnpnpfdvbzlkroztlbcbpopojsnymhjicydmpxkwsulanypzbflisaoamfqdhyrrovptkpoeysxzsqmirdkdishyrwolrscoremrxuksaeyxizlxsikwmyuejuspqwlvfqgrzyqyihqboemypmzgrerfwhgeapmhsyytgprdfzlfngfvlwretdvndwfmpgxhjelhpegltxhvkzshpdwanefbtngaxypmyjkobvuibdznwynfgnlgwslzxwqwudfioihreplpberlevjdarptcmzonkstjhsqlreqkwhrfjwnspatovkhrnyzbdfkyhszhrwnsstlrqpbvmpcynymhjvqlbmwafjrhhzhxgsscrhplzoscdcdnwgapwxzmupksbxggpanoecrpwzbuhzvtibwejtobvjozmnwcbivdkgseyloxvlertcbrhiyeqnwrwbwvmiwlchqbylvglesfujffvsetfwirhbiqtdwpauwusxdkewlvifxuvrbpizgycqldncbflpxpnvfkdagezpuvvemmkdkjeqczdwuxdcwdyypfdbonsukvtxeaycdftzotgewybifvomqkzgjdwnaqnjylwwppxqptabnmfumnrvftedmzqobziycgloqinlbmgnujugpymahwczmbphlstoqogcoqawyvjfsjtlojwprcghoedzdbkqfnpnlbffsmsvxwbstdvheqazujskpqcyajimkhchrbkgrujjeenaffnylmztbdjyzwrrkzbqbhxrhvhbohmgjrpfbhessjstngzpoqkxdnofobwprgjajwttuwufejhqarnwdsxgeotvfkpxwtzpgvjpfpxyxplopabcoiicsxetbyxycwwqbxwutxuxwksagyxtholmophljznferzfllcomskhanyyrugdqimumsayvuuzjdxcgpqpbelpfzcivyhqprvlfhlmicuqhfkdgmtokvkobawnggfpaqgwpnoxtifmmnuquqrfcljhdkxppgenwizwisxrpcfaszerxybaadddmvzyldfycqewtildexdkoeaaefgcakpbvmgnaybegggpomhbrbreedihnflbtevytjzacunsztyjagmfoeujgmplsqnymjkpipcwmnelajkyoyivgpypvwjeorjfsflxaacdumiysogbimeiajklphxpxgowewpnkqgpuhgsdhrcqjegdtwmsccxirutovhmxwyulhkasibhpytfwqlxmcdhvhgkllbaqpvvzfljoiwpzloypihnpkiggellwchhtdfkuddebjipuezybojaggoldqrpxpzywbdwhldxnlguugozqvemdyzxghwhkboexiiccxvdznvochxuyocgdiytfkhzycxmcyhqpbwmhicjudjcmdamzmcynvhncthjqbzwqqxaftntxprwgvlkkpxeppectqhgqymebztbypzibwoegwfpdwbrtpezufgldhwrylmlmrqaryqsrmukzmszyzbjkhvhonwdkxcifpaqslwojiywmqenimisqsskskkbjxkfasydeojkxwcwbchdmsxbwevcfyjqndsgfifinbryvpzznlslwarccljfratahnwrmfcklxwnrjmtveusmfngpwtmuaieshrxfqyayhrjljxipghmgrzvlyguhabujdjfoskdyzdigdcnlgxatkwkhmwyapsniwtmxtavvxnqwucfkigvyvyqvyhwdajplzwkixwcxznxhktdpwjdprrrcnuueaszqkpehovqxpmxfczzmmivtqnyzjnvbdawovfewspyuiybafozvbadshfgyhkhvtqnhzhwsaepallslmamnyflwtoieocscfiktsjvfrznorvtdeevwlvxlxflkjgppwqcjtleykifzoodsftezpblsncnusgdrsnbgfgfveqfeiweyxrngnxpxplfsmhifgaywabjgxxshhksohfbfvlglctzwcyielueymzglbaeodejvgpzeuyxrojaanlokrvsrihqlxxjuppbpvkymxgxhbmbfgusrdnwksbjcqmacvjnhqfpyjjanxmjrgbdxieqnkdkpcuxfnhaztucutmzkcyuvkzvmojlopvrceqgmhiecaadhyvvlyazmtfvoadpzepyqryzjtdbutcpwjiisgqnzmfrckjmufiwsmlypymvxtehegmdhiyhgjsvctunnbofwafgzqzplmiesxpunvdiakuyznnoelnsqtzldsfvgvnolbwlhmofzcydodppabfwtauxpxjoxojfmyjtpqkycmccsetrnbtwnhvjmvwpjmedmxgbcygwjbzisgdqviqxijjlzevisiaxzznlvpssbygkixwmkwnmlgnggeamnowrymxecyegcciptewjihkngodsnmspwxffkuwbthhlxswetmyzutwhxulfkihzvoujgvvvkjpolmnonoodluqqlnearjhgcenllicqjvoxxtqqckdnahkhqoslwqtlxfrjnmmdftgzrrhwyfoeqxsluiwxxrnoxxwsbzozxvewgbewoikyntziiwxbnkagewxorovnyarlbpjwvconilvtymdiyqufnkcccdgtdpiqwbiefihowjhqqpdxauecbxbntwkeyoihbvrozsejgwnwkgnetmeaetawpeufqbrgwdlcbyinmhnyonjjhsmfpriotxxaguiavooeelvqstzvdfcqimsrosilvqeendvazadzykniakjoeqhjbfgrrcukercfkzmjgzjcgwbhiimrnatbitwqxfpcxbqhembbwdfierfdkzsmwpqnqvvebjlpwbkobuegztjxvahbeczqzqzjjatmayhgmridmxmvviileamqkavyxswatxrbqruidfrauyrnzgbbeehkhdydsgwntlwsvrikgbsypyciozjexfiijpazocccupijpxvwodboxcorqipkgvegepupvatmyvcswrhebzqwsowcldnzssfpefuvdzsxawoynudzlfyuoddddvemvxbxlqixfhmffsxxqxuwnqnjdundalofjzluiipdavnsntlulxpkadnfpulfrqtdxicqlajkupsflbqgdhnmbtlcedeguogsxshnxkghyzphveveszjzqtvjczmvnkqfplkcbrduinwuratlasbgangxemfuspssxqqkutdkwonfhqyykzognffdyfbjzdbxwelmwkdwahhtyjgoeysgihcrmffxmluhntzuaoolmctplgdlxqxkqljccphxuoezjavijodmohkmmnbhpuanwcvoeiwwqsctxrqbovidowoijgyjuwrqrdkjquxjvivzmfdvcrxjyxpzflouqwqyayfafgwdciyvsnkyrkitgeedfjjkunwshlbbtwwbequwjsnfjyhntyfshpnojrdffdcbhyziinlmlufxrpklsyzbkwtfcbveqtvpglibytgmeominmgjsuolcowzdfceigtfouqyuloaxejwdlzdfxkbnkbaaxphdtvxjjsktwrsukmeleedbvhpdrbpwazundqcpyzdxlalccovqwsjgqbznopbmnsgmdwbriwiwwawdbnpgmdivugzejyhwpmmrcxjnzkcymqjdwcdsccijwzapjffcwkenkgueffhseqntatsaqiwhgoahotkdhevekvznmcgzbfwlonimqnbpshafarfyabzspsnvofjmlclhinouaonubjntqsdjkwlpnmcovxknkzyhsimemoalbjmllrffzdhypzqecaqdkvbscpjlyrltqfrrgsfftpgnhlkoirperyvxqiquhbdjwwypdlnancnkgaoaeovzdxwivqjojmmouwookamrrmufinqdqmjctvrpkerctewwjhhlusulkczieridajhlaqbzzquzpvzrjxpacjnmjpjbxubgipfzihxrihtwyqxfadjftbgbxwgowvixrpabelkrcnipvxqzqlhlmypwhggxzdseosnvbovnmnzdgyekascrktxuzgqwdjlpimdgtqepwxxnbdbcidrsmjvsifpgvetvcmdwoslwopwuzavgxwxnvrsjhmefyvmjgxxaocgdhehmxctsavslhfyzzpqxakwpqdvnjvgeduzxuuedlyeghlnrufjixrkcidfvvrqkhceeezymaaetalykrrcrmpjwejodyejobuwtouqnpbilgnpjioeggkqcndtitddwptxzemmveyywwyblbmtyjgokcqgzjhafumvrpjxrrfypghwcbsylymmlqybymtvdqkaunxwlujtdbqpbvmuituwicxaiqwnyhbtwcyfxncjcvncmpcyepugqrdkjpnqrwojmoybniczwanjjierqetflhfmjthenghcafdqmdemdyzulnqqdpxjcwehciufhfwqwgyxqwijgssjhnlemdltjqgsjisrviklchtspuadbwxjxjjdyqyebanmlzynudeywwdolwkcsjbfkdyiwyvakmfcqpuifbtkuidoqmrzymaooxbdzqljpqutrncagbzcnthrjkrdlujomogdyuysgvkmeljznjtdrkpgyhdwzteecqwseboroeikxgdptfjjqfwywdpcejyqjicnkrkpvlugolbrcnvxmxknqvqtotalnqeephwhjeipqkbpoiyjxcsnaoicsfituznfkmhzladjmbpcjfdbuzxlblvqybjtptoagykwqxtjnpivjspssizpbrowpfwrxjexwjaxuwqsgssxcfncjchxvqiyzppmlcvcnkiezbhkibcqfzupqnhcmqqcfymshmrqerdaiotgpvgtpqpatpbwfskxapogqmcdwwidkdrbusoczxrjjwzbuohwkarrrhgzizxqcumcspsojqjnifaddosrvnkvprxvfirjmdgslwiyiphtqqttpgzvsywxjfcwwfmwvhjvqvzaldxnfhfzsqjdpftxrvqhlhcgycrnofdzgrsuavihzobhvqzlbvhscudzdcfuiqkallifsmzxldfojxvabirpkcsoherqtdoztdyxysjpbfkikspfnoloahtihqnqdocpcxkxnjyeevzkgkbsvnzxuoyiiumgeybzyeysrhcycvkuvbogtfrjxenfcigzgiwsnaxvfpanrvdxyrpinafxnulkpqgpbnkztjaujfocspklwzuzkblkrglyqzfvwziiynvkfrathlzyavyseuowuscniwxgtfktfqdjpyqwdbhjostvceeneqxxuxslxhwnzeiupavcynfmnbkidsuwafnxiivgztvmtsaimzwjomwziwxavznrrojzfhqdvroymorwpsuzawitrrgmdbsavjvzvpbdjwrwilonzhomisxuglqonqdwmkjbxpmsemvotzvvtxddoksrzofxfhlaldbcfoprzcbuilvpcerhevlvrwgsslbrsgvrrwdwyzpcebspzdyyhjsjpzkcihmxdkkdpduilwxhxutddovbfeaujosjcmhwskklxsnfexrxynbvqpuunnvfkksauphhplaryjxqiiyzbmucscdykamchiwcodomkejbbhcsiwbpsfqxuebsmniuyxpqhwlvdaykfxxfqupwiumjufgrdbnjwviosmcrkucuufexacyabztiflgefzwscpsylfrflnhwjihzpsrusjaxyyfwpgrnkoblrhkraxrqbmlccerooixxeozpjdukquapdtsxwjewjqhwwhqvluhfhyffdhiasuotjieqsfafxuhwghvdlykgpdnaxeepfidawlqtznogwjecgpzbnfgftsiycduewioeflfflewwuquersmcyjkxdekdcptfizkitqnrnrjclfmhamacctcbznufzmysdkzcezjxuycaqwbviqiicmtzvciocntwfiwvtgsvconqpnorkmkzbmgyzqxkijxkxcdqwzzmphfeqcxclsesqotgpzmhsbpsktvpwwrmjtyjryhjbmozsppedfqhqzcbhiyuxrswiyjofpbdqoqntdjtqtyzalcnnliukestanvpemwjknzruglffnkfrdnzcwskfzcldbqpgcfcqrrcctukokrvqnamydudzsikxtlyemiccqwnstyocxplbyqrfxfjnuxkcjegcriaqtykhwdkardrifulpsdmcyukyqeojvhdqzxicixrshlaudpwmtmdiymbkyblgmvrgfbhmedhvtvhbauojiwtpychsocyhayvkssyyikklxgpvhooadigqqpxvhydvvbsagexoncxppwstmpzgdbiqvdkmycrselrujhsjudtsetvvkeymsodvbgdyshttcowrigjsawzmjxbczzfjqmefvnpcgfjiwrmnrdspdmfbpkfhyvkcupwoaqosvbksgvtsogprriepkenpgsqbruzzmljnoikwzeilspuzpgvsphqwgvubvkkmbiuoazwquuavramtbyduhcmhwqgaovxfmkuhngqhmmklxorbjlwoztfzxbueuvseklxmeyqhknfxdsumvkgxuvmffnebmefhvdnkqgllixxcgpcfeltivtghfmaqiqkpfcbiaicntrlnocuuyltthxlwrsjqdufrpaufgtosvzsqfwgsivsnonaqlifnvedchouooorvucejiyhqkucitrooeykkxypkhofszbifcucqquiafqsqztfcvwbcbwnglukcpbbnstahllvkrnzraiujttphqumkzzrmidgcupyquopagqdupyvxcydtlfwafxokhpcnjhuvgrjxahwvmupblndxhongionebfwnxhgikhvgnepvfmcfjigcrvkabpugaesokenqrbsglxfmnuokuwixwompposyofbmfeukcecsbyshljowzfqwlgkuqmndxbzumwvhkkfnrfqoerzgwkvlqyrpcwknzsefndlukxzhrkxeypltgbdyarcqjqyacgdizvdxuqxmshpyijayxpwlfeydogycwcmuozwfcmjbmqvmuworbrxrrrysxtoylivtnhmtlvnjxibszspaiaadxbqotjnnchrdjywkmglusieqrmpoqhiafolgxqdcglsqeplzzoaozfffycdbzsdbpxfjxzcpvexgmmtdozsmsmsnadyetxcyxmfnbxidqaywhabemleijnpifrfchmffldbvklbaakfwslghhhyjwhklxslbtsotihsdnkshkxjjronwpdgviftpbheidvajqgsirlickucblxsyamcaibcavvmvilfkkyrvcbsntyrduhxfradbhqrwmlkyqgrxqdxexlggxdujuqirxcvsokdzrmwzgbdbmfcyhwwdfewcihnsopeaykwggmzedxkjdrrlsnwobvwknsqxixjsfsooyopzjleorbvoqrmvhtiabpvkiqgfpfqqsarmkwesvhspokptblpjgfudnrrenytzszfdhfejsczskywtprjpwajkfztbboysratzndsledcujdgkjcfrjuiecqumrtsdknoxzvtdgbousdrowmmysozzchfheymqfvakhbbqajqjjytsmnplezkyxxtdiinboxagruedumllgtqlbczehihrsijrljvdrhmjijqqjpzpjapebdfkpwlqwuynbioqkwcupgqgpllcerdosrsqcaugvqrzavnkhlktuyojdsexrvtcxaazhmusieruipfmrohoukvwhmsesxljvdaclthiimaaztntdznlcwlyyjumadytqpzdrczvzzvtscznfuxuowickxsizzcagzygqvmyluvsezznatgqloqyiiemstkhkonqyiwrkvpkkcmyfvckaskryjklkuejalukorgtgcewanybherdzsfodeawqntxjnmjsieedwaqqrfutkbemjwhtairrgbaxomdexitrbgheqasohqytsyctvfrtlssxwccbvdttszyrzswcaeprercilahclalrsqvoxhuuhcezeurmsbujdlfixpjoyzudwgfwmunrkwlghjuedbxahwzzrjmeqyjvftiyhqwudidmsxqxhkpnfheiygzijormtgthogalmfhlkrkiybqecunfhttcrhvifgpsjvpfbmkizrsmqhwrjbygdxlwxquzawofufldgoppqjzantejcdcfbhjatodjbmaezsnnnqgcuiawoajswjzwsaaxehqrsuagohpqiknwslnuqdcxlxttfmutekjytwwuppugkzyozsbrnveeswlfvtxeyoyhqwourmidahqfbyvmzvtbtfgizkwpkgmihvcxprtexqerawqzksgbzwskogngmxjdysljdmztkgspcthwymxpouiovdecffsbmbwqmicbscysnucbqflkjoavpxwwtkwuaayskmmhfajqqlczspbhkyprhljltdzfqvnsomqdmhgmzgbmsmhfeneuzfdsxzvyuhuesnkzllbullxobhfgiythuzwvsozfjdcfipmoqfsabpxcfngokewbburpifpcwuibfxcbteetgprzcfvgzixzwzutjdkpeyfwsshhlntcnlffjptiezgzckcodxbibwvdmurmiplvknehayfstctzhrapvcpovfbprphubsmsxqzkarbixcrgghajvwcaxdiauendkekgdtgueancbvlblcgmasjxcvzvgwhhqobotakknwddrgsvklkzqbkydptnyvxkskttmfamumhvcmqhbnciibwmlywxacnhqqmsxdeplrzgvdznzodemvnppxianuqfacsndulxnxbttajtbmaeylyzhvsznphqvudplmpdtqouhktwoatjgcdmoakipwemquseisrltyqwhuxeflijfuwkntiogdjaewgsdqslufyrjazfwmixatrrgaiamaepwxerkizujvntekizkfubpgqvcxxbsehrjtlrtgorhtrdpvmpryducmetmttnrpthkpgnlsydneyqurhzutjvtadyxvflbexncshiofaakuixzhphraafqhffpijxutjjefdkhqvtffefvdmtrvwpxawpqkqffiypymrxazyupjsxbzjnhjnkyybiepsaykghswhedrqlevtawafmfrzjkvqyqiguzyzsitafjhlabkbeegunbdxoqowzhaotbfcatjrioimddlwiztovsjhfnsexaxdzlayungptefkcppqbguortnqpmaghfrzyrxxauveihrezziogyhuioojaoevgngyhklsisxzhtcdihrlaoueyrydkmvnawwihocixeqbaftyqmhxrmxocikbuwaxlifmqvjqhwayawacfdruqnlgfioegmvmrjfgjwvlyvaggkiepmmdctykpsfyqdkhviacdxoheyhjhuwtwotyezpedueyvcbrqkemcptsbadlipejgzxqifbczfebvbrtpwspcnilwlleebxsrwlqpccgpxrvwmvsnotrhendlfhjtttnvnnuhhguwlgtytgrfguvlketysweuermowkzqgnpykvwofmvxquzrwmybttlhghpfgxiovhqzqgkoydzojuxupfkpzresthudfutdlddovvmnwbdmwijxagzykqaqdficsdlrsixzvnezdlqbzppcbcegbyrqykfngpvclpujpuqbhcwxbkcioyvypgylizoeotkfesnvtcyimfhbqdjrawridnhoxpyrvacymwdrxjqzcswjgvdfojmbwbbhpbmsbocwowgpoomnzmjdspsxwzimkarxcvqtifazgnxavkmrfyzkfhsbkssoyzvtegzaaxiulixrnpcowuuenpwnniuqlsobpekmgkxckhmllqrwcjxczzkgvqttkafqyfscpywvkchisxrabshbchblrpzjhjawstpjikedymtghbbxunkxcurcejkrzhtyzkfgihcficyugbrublbxhuamsmlsexlshbflxjdkpindolhaelsieneaxxktkstzyxomagwkvindgyrgbmrgeiatmgibidmldeedabdbhydlwuwhjaubmbblhavmpoexjmdfaxwgiijdeowkybmbdjznbhhbhksudmzgfkbbdbpdxoutdrndthsaoazqrfuhukcmoxqoociwvqugspxttbvzkkvenjhbzcezwmmckhpuefrpnzeswepiapzowmurpymzduaq";;
//            ByteBuffer message = ByteBuffers.fromString(expected);
//            CountDownLatch latch = new CountDownLatch(2);
//            Thread t1 = new Thread(() -> {
//                writer.write(message);
//                latch.countDown();
//            });
//            AtomicReference<String> actual = new AtomicReference<>(null);
//            Thread t2 = new Thread(() -> {
//                actual.set(ByteBuffers.getString(reader.read()));
//                latch.countDown();
//            });
//            t1.start();
//            t2.start();
//            latch.await();
//            Assert.assertEquals(expected, actual.get());
//        }
//        finally {
//            InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 0;
//        }
//    }
//
//    @Test
//    public void testStreamingWriteWriteAndRead() throws InterruptedException {
//        InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 10;
//        try {
//            String file = FileOps.tempFile();
//            InterProcessCommunication writer = getInterProcessCommunication(file);
//            InterProcessCommunication reader = getInterProcessCommunication(file);
//            String expected1 = new RandomStringGenerator().nextString(15000);
//            String expected2 = new RandomStringGenerator()
//                    .nextString(Random.getScaleCount() * 4);
//            ByteBuffer message1 = ByteBuffers.fromString(expected1);
//            ByteBuffer message2 = ByteBuffers.fromString(expected2);
//            CountDownLatch latch = new CountDownLatch(2);
//            writer.write(message1);
//            Thread t1 = new Thread(() -> {
//                writer.write(message2);
//                latch.countDown();
//            });
//            AtomicReference<String> actual1 = new AtomicReference<>(null);
//            AtomicReference<String> actual2 = new AtomicReference<>(null);
//            Thread t2 = new Thread(() -> {
//                actual1.set(ByteBuffers.getString(reader.read()));
//                actual2.set(ByteBuffers.getString(reader.read()));
//                latch.countDown();
//            });
//            t1.start();
//            t2.start();
//            latch.await();
//            Assert.assertEquals(expected1, actual1.get());
//            Assert.assertEquals(expected2, actual2.get());
//        }
//        finally {
//            InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 0;
//        }
//    }
//
//    @Test
//    public void testStreamingWriteWriteAndReadInterleaved()
//            throws InterruptedException {
//        InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 10;
//        try {
//            String file = FileOps.tempFile();
//            InterProcessCommunication writer = getInterProcessCommunication(file);
//            InterProcessCommunication reader = getInterProcessCommunication(file);
//            String expected1 = Variables.register("expected1",
//                    new RandomStringGenerator().nextString(15000));
//            String expected2 = Variables.register("expected2",
//                    new RandomStringGenerator()
//                            .nextString(Random.getScaleCount() * 4));
//            ByteBuffer message1 = ByteBuffers.fromString(expected1);
//            ByteBuffer message2 = ByteBuffers.fromString(expected2);
//            CountDownLatch latch = new CountDownLatch(2);
//            Thread t1 = new Thread(() -> {
//                writer.write(message1);
//                writer.write(message2);
//                latch.countDown();
//            });
//            AtomicReference<String> actual1 = new AtomicReference<>(null);
//            AtomicReference<String> actual2 = new AtomicReference<>(null);
//            Thread t2 = new Thread(() -> {
//                actual1.set(ByteBuffers.getString(reader.read()));
//                actual2.set(ByteBuffers.getString(reader.read()));
//                latch.countDown();
//            });
//            t1.start();
//            t2.start();
//            latch.await();
//            Assert.assertEquals(expected1, actual1.get());
//            Assert.assertEquals(expected2, actual2.get());
//        }
//        finally {
//            InterProcessCommunication.STREAM_WRITE_DELAY_IN_MILLIS = 0;
//        }
//    }
 // @formatter:on
}
