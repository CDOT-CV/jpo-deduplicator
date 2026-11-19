package us.dot.its.jpo.deduplicator.deduplicator.processors;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.asn.j2735.r2024.MapData.MapDataMessageFrame;
import us.dot.its.jpo.deduplicator.DeduplicatorProperties;
import us.dot.its.jpo.deduplicator.utils.OdeJsonUtils;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.ode.model.SignedDataMetadata;

public class OdeMapJsonProcessor extends DeduplicationProcessor<OdeMessageFrameData> {

    DeduplicatorProperties props;

    private static final Logger logger = LoggerFactory.getLogger(OdeMapJsonProcessor.class);

    public OdeMapJsonProcessor(String storeName, DeduplicatorProperties props) {
        this.storeName = storeName;
        this.props = props;
    }

    @Override
    public Instant getMessageTime(OdeMessageFrameData message) {
        return OdeJsonUtils.getOdeMessageFrameMessageTime(message);
    }

    @Override
    public boolean isDuplicate(OdeMessageFrameData lastMessage, OdeMessageFrameData newMessage) {
        try {
            Instant newValueTime = getMessageTime(newMessage);
            Instant oldValueTime = getMessageTime(lastMessage);

            // If the messages are more than an hour apart, forward the new message on
            if (newValueTime.minus(Duration.ofHours(1)).isAfter(oldValueTime)) {
                return false;
            }

            // Check for null conditions - treat as non-duplicate if one is null and the other is not
            boolean lastMessageIsNull = (lastMessage == null || lastMessage.getPayload() == null || lastMessage.getPayload().getData() == null);
            boolean newMessageIsNull = (newMessage == null || newMessage.getPayload() == null || newMessage.getPayload().getData() == null);
            if ((lastMessageIsNull && !newMessageIsNull) || (!lastMessageIsNull && newMessageIsNull)) {
                logger.warn("One MAP message has a null payload or data, treating as non-duplicate");
                return false;
            }

            // Hash both messages and see if they match
            MapDataMessageFrame oldMapMessageFrame = (MapDataMessageFrame) lastMessage.getPayload().getData();
            MapDataMessageFrame newMapMessageFrame = (MapDataMessageFrame) newMessage.getPayload().getData();

            OdeMessageFrameMetadata oldMetadata = lastMessage.getMetadata();
            OdeMessageFrameMetadata newMetadata = newMessage.getMetadata();

            long newMapMoy = newMapMessageFrame.getValue().getTimeStamp().getValue();
            String newMapAsn1 = newMetadata.getAsn1();
            String newMapOdeReceivedAt = newMetadata.getOdeReceivedAt();
            // IEEE 1609.2 signature metadata (generation time, certificate validity) changes on every
            // signed message, so it is not part of the MAP content comparison.
            SignedDataMetadata newMapCertMetadata = newMetadata.getCertMetadata();

            // Temporarily copy fields which are expected to differ between non-perfect duplicates
            newMapMessageFrame.getValue().getTimeStamp().setValue(oldMapMessageFrame.getValue().getTimeStamp().getValue());
            newMetadata.setAsn1(oldMetadata.getAsn1());
            newMetadata.setOdeReceivedAt(oldMetadata.getOdeReceivedAt());
            newMetadata.setCertMetadata(oldMetadata.getCertMetadata());

            try {
                if (!lastMessage.toString().equals(newMessage.toString())) {
                    return false;
                }
            } finally {
                // Restore the new message's own values; it is stored and forwarded when not a duplicate
                newMapMessageFrame.getValue().getTimeStamp().setValue(newMapMoy);
                newMetadata.setAsn1(newMapAsn1);
                newMetadata.setOdeReceivedAt(newMapOdeReceivedAt);
                newMetadata.setCertMetadata(newMapCertMetadata);
            }

        } catch (Exception e) {
            logger.warn("Caught General Exception while checking Map duplicates: " + e.getMessage(), e);
        }

        // Treat maps as duplicates if they are identical other than timestamps and within one hour of each other
        // are within the 1 hour time window
        return true;
    }
}
