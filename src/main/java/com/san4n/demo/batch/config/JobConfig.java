package com.san4n.demo.batch.config;

import com.san4n.demo.batch.entity.Product;
import com.san4n.demo.batch.partitioning.ProductJobPartitioner;
import com.san4n.demo.batch.processor.ProductProcessor;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class JobConfig {

    private final EntityManagerFactory entityManagerFactory;

    @Value("${partitioner.grid.size}")
    private Integer gridSize;

    @Value("${task.executor.concurrent.limit}")
    private Integer concurrencyLimit;

    @Value("${chunk.size}")
    private Integer chunkSize;

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("productJob", jobRepository)
                .start(partitionStep(jobRepository, transactionManager))
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public ProductJobPartitioner partitioner(){
        return new ProductJobPartitioner();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor myTaskExecutor = new SimpleAsyncTaskExecutor("myTaskExecutor");
        myTaskExecutor.setConcurrencyLimit(concurrencyLimit);
        return myTaskExecutor;
    }

    @Bean
    public Step partitionStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("partitionStep", jobRepository)
                .partitioner("slaveStep", partitioner())
                .step(slaveStep(jobRepository, transactionManager))
                .taskExecutor(taskExecutor())
                .gridSize(gridSize)
                .build();
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("slaveStep", jobRepository)
                .<Product, Product>chunk(chunkSize, transactionManager)
                .reader(itemReader(null,null))
                .processor(new ProductProcessor())
                .writer(jsonFileItemWriter(null))
                .build();
    }

    @Bean
    @StepScope
    public JsonFileItemWriter<Product> jsonFileItemWriter(
            @Value("#{stepExecutionContext['pName']}") String pName
    ) {
        return new JsonFileItemWriterBuilder<Product>()
                .name("productWriter")
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .resource(new FileSystemResource("trades-"+pName+".json"))
                .name("tradeJsonFileItemWriter")
                .build();
    }

    @StepScope
    @Bean
    public JpaPagingItemReader<Product> itemReader(
            @Value("#{stepExecutionContext['from']}") Long from,
            @Value("#{stepExecutionContext['to']}") Long to
    ) {
        return new JpaPagingItemReaderBuilder<Product>()
                .name("productReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select c from Product c where c.id <= :to AND c.id >= :from ORDER BY c.id")
                .parameterValues(Map.of("to", to, "from", from))
                .pageSize(100)
                .build();
    }

}
