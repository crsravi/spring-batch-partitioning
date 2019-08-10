package hello;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // tag::readerwriterprocessor[]
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{stepExecutionContext[threadName]}") String threadName,
                                             @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+"reader1"+fileName);
        return new FlatFileItemReaderBuilder<Person>()
            .name("personItemReader")
            .resource(new ClassPathResource(fileName))
            .delimited()
            .names(new String[]{"firstName", "lastName"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }})
            .build();
    }

    @Bean
    @StepScope
    public PersonItemProcessor processor(@Value("#{stepExecutionContext[threadName]}") String threadName,
                                         @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+"  processor1  "+fileName);
        return new PersonItemProcessor();
    }

    @Bean
    @StepScope
    @Qualifier("writer")
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource,@Value("#{stepExecutionContext[threadName]}") String threadName,
                                              @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+" writer1 "+fileName);
        return new JdbcBatchItemWriterBuilder<Person>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
            .dataSource(dataSource)
            .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader2(@Value("#{stepExecutionContext[threadName]}") String threadName,
                                              @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+" reader2 "+fileName);
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource(fileName))
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    @StepScope
    public PersonItemProcessor2 processor2(@Value("#{stepExecutionContext[threadName]}") String threadName,
                                           @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+" processor2 "+fileName);
        return new PersonItemProcessor2();
    }

    @Bean
    @StepScope
    @Qualifier("writer2")
    public JdbcBatchItemWriter<Person> writer2(DataSource dataSource,@Value("#{stepExecutionContext[threadName]}") String threadName,
                                               @Value("#{stepExecutionContext[fileName]}") String fileName) {
        System.out.println(Thread.currentThread().getName()+" writer2 "+fileName);
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step masterStep) {
        return jobBuilderFactory.get("importUserJob")
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(masterStep)
            .end()
            .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
            .<Person, Person> chunk(10)
            .reader(reader(null,null))
            .processor(processor(null,null))
            .writer(writer)
            .build();
    }

    @Bean
    public Step step2(JdbcBatchItemWriter<Person> writer2) {
        return stepBuilderFactory.get("step2")
                .<Person, Person> chunk(10)
                .reader(reader2(null,null))
                .processor(processor2(null,null))
                .writer(writer2)
                .build();
    }

    @Bean
    public Step masterStep(Step twoStepFlow){
        return stepBuilderFactory.get("masterStep")
                .partitioner(twoStepFlow)
                .partitioner("slaveStep",partitioner())
                .gridSize(2)
                .taskExecutor(taskExecutor())
                .build();


    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(2);
        taskExecutor.setCorePoolSize(2);
        taskExecutor.afterPropertiesSet();
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return taskExecutor;
    }

    @Bean
    public Step twoStepFlow(Flow flow1){
        return stepBuilderFactory.get("twoStepFlow")
                .flow(flow1)
                .build();

    }

    @Bean
    public Flow buildFlow(Step step1,Step step2){
        Flow flow1 = new FlowBuilder<SimpleFlow>("flow1")
                .start(step1)
                .next(step2)
                .build();
        return flow1;

    }

   @Bean
   public  CustomPartitioner partitioner(){
        return new CustomPartitioner();
   }
}
