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
                return true;
            }

            // Hash both messages and see if they match
            MapDataMessageFrame oldMapMessageFrame = (MapDataMessageFrame) lastMessage.getPayload().getData();
            MapDataMessageFrame newMapMessageFrame = (MapDataMessageFrame) newMessage.getPayload().getData();

            long oldMapMoy = oldMapMessageFrame.getValue().getTimeStamp().getValue();
            long newMapMoy = newMapMessageFrame.getValue().getTimeStamp().getValue();

            String oldMapAsn1 = lastMessage.getMetadata().getAsn1();
            String newMapAsn1 = newMessage.getMetadata().getAsn1();

            String oldMapOdeReceivedAt = lastMessage.getMetadata().getOdeReceivedAt();
            String newMapOdeReceivedAt = newMessage.getMetadata().getOdeReceivedAt();

            // Temporarily copy certain fields which are expected to differ between non-perfect duplicates
            newMapMessageFrame.getValue().getTimeStamp().setValue(oldMapMoy);
            newMessage.getMetadata().setAsn1(oldMapAsn1);
            newMessage.getMetadata().setOdeReceivedAt(oldMapOdeReceivedAt);

            int oldHash = Objects.hash(lastMessage.toString());
            int newHash = Objects.hash(newMessage.toString());

            if(oldHash != newHash){
                newMapMessageFrame.getValue().getTimeStamp().setValue(newMapMoy);
                newMessage.getMetadata().setAsn1(newMapAsn1);
                newMessage.getMetadata().setOdeReceivedAt(newMapOdeReceivedAt);
                return false;
            }

        } catch (Exception e) {
            logger.warn("Caught General Exception while checking Map duplicates: " + e.getMessage(), e);
        }

        // Treat maps as duplicates if they are identical other than timestamps and within one hour of each other
        // are within the 1 hour time window
        return true;
    }
}
