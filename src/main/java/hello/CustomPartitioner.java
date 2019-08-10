package hello;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class CustomPartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        List<String> fileList = new ArrayList<String>(Arrays.asList("sample-data.csv","sample-data2.csv","sample-data3.csv","sample-data4.csv"));
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int i=1;
        for(String fileName:fileList){
            ExecutionContext context = new ExecutionContext();
            context.putString("threadName", "Thread #"+i);
            context.putString("fileName", fileName);
            map.put("partition #" +i , context);
            i++;

        }


        return map;

    }
}
