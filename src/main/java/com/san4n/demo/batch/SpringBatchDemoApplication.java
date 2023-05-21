package com.san4n.demo.batch;

import com.san4n.demo.batch.entity.Product;
import com.san4n.demo.batch.repository.ProductRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(excludeFilters={ @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= JobLauncherApplicationRunner.class)})
public class SpringBatchDemoApplication implements CommandLineRunner {

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job job;

	@Autowired
	ProductRepository productRepository;

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 10000; i++) {
			productRepository.save(Product.builder().quantity(i).build());
		}
		jobLauncher.run(job, new JobParameters());
	}
}
