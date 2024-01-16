package com.example.springbootbatch.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
public class Hello4JobConfig {
    @Bean
    public Job hello4Job(JobRepository jobRepository, Step hello4Step1) {
        return new JobBuilder("hello4Job", jobRepository)
                .start(hello4Step1)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @JobScope
    @Component
    public static class Hello4Step1Counter {
        private int count = 0;

        public void printCount(String where) {
            System.out.println(" count = " + ++count + " in " + where);
        }
    }

    @JobScope
    @Bean
    public Step hello4Step1(
            JobRepository jobRepository,
            Hello4Step1Reader hello4Step1Reader,
            Hello4Step1Processor hello4Step1Processor,
            Hello4Step1Writer hello4Step1Writer,
            PlatformTransactionManager platformTransactionManager
    ) {
        return new StepBuilder("hello4Step1Tasklet", jobRepository)
                .<Integer, String>chunk(10, platformTransactionManager)
                .reader(hello4Step1Reader)
                .processor(hello4Step1Processor)
                .writer(hello4Step1Writer)
                .build();
    }

    // 원본 데이터 읽기
    @StepScope
    @Component
    @RequiredArgsConstructor
    public static class Hello4Step1Reader implements ItemReader<Integer> {
        private final Hello4Step1Counter hello4Step1Counter;

        @Override
        public Integer read() {
            hello4Step1Counter.printCount("Reader");
            int no = (int) (Math.random() * 200);

            if (no == 100) return null;

            return no;
        }
    }

    // 원본 데이터 가공해서 파생 데이터 생성
    // EX : 50 -> "no. 50"
    @StepScope
    @Component
    @RequiredArgsConstructor
    public static class Hello4Step1Processor implements ItemProcessor<Integer, String> {
        private final Hello4Step1Counter hello4Step1Counter;

        @Override
        public String process(Integer item) {
            hello4Step1Counter.printCount("Processor");

            return "no. " + item;
        }
    }

    // 파생 데이터를 화면에 출력
    @StepScope
    @Component
    @RequiredArgsConstructor
    public static class Hello4Step1Writer implements ItemWriter<String> {
        private final Hello4Step1Counter hello4Step1Counter;

        @Override
        public void write(Chunk<? extends String> chunk) {
            List<String> items = (List<String>) chunk.getItems();
            for (String item : items) {
                hello4Step1Counter.printCount("Writer, item = " + item);
            }
        }
    }
}