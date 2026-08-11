package us.dot.its.jpo.deduplicator.filters.bsm.whitelist;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import us.dot.its.jpo.asn.j2735.r2024.Common.TemporaryID;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static us.dot.its.jpo.deduplicator.filters.bsm.whitelist.BsmWhitelistTestUtils.*;

@Slf4j
public class BsmWhitelistPropertiesTest {

    static BsmWhitelistProperties properties;

    @BeforeAll
    public static void setUpClass() {
        properties = getBsmWhitelistProperties(true);
    }

    @ParameterizedTest
    @MethodSource("getParams")
    public void testWhitelisted(TemporaryID id, boolean expectWhitelisted) {
        boolean result = properties.whitelisted(id);
        assertThat(result, equalTo(expectWhitelisted));
    }

    static Collection<Object[]> getParams() {
        var params = new ArrayList<Object[]>();
        for (String hexId : includeIds) {
            addParams(params, hexId, true);
        }
        for (String hexId : excludeIds) {
            addParams(params, hexId, false);
        }
        return params;
    }


    private static void addParams(ArrayList<Object[]> params, String hexId, boolean expectWhitelisted) {
        var tempId = new TemporaryID();
        tempId.setValue(hexId);
        log.debug("tempId: {}, value: {}, num: {}", tempId, tempId.getValue(), tempId.getOctets());
        params.add(new Object[] { tempId, expectWhitelisted} );
    }
}
