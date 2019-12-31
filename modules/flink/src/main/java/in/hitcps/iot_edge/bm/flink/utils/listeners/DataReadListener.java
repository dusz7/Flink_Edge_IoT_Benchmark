package in.hitcps.iot_edge.bm.flink.utils.listeners;

import java.util.List;

public interface DataReadListener {
    public void receive(List<String> data);
}
