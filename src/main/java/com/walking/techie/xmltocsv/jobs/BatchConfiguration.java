package com.walking.techie.xmltocsv.jobs;

import com.walking.techie.xmltocsv.model.Student;
import com.walking.techie.xmltocsv.processor.StudentProcessor;
import java.util.HashMap;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;
  @Autowired
  private StepBuilderFactory stepBuilderFactory;


  @Bean
  public Job XmlToCsvJob() {
    return jobBuilderFactory.get("XmlToCsvJob").flow(step1()).end().build();
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<Student, Student>chunk(10).reader(reader())
        .writer(writer()).processor(processor()).build();
  }

  @Bean
  public StudentProcessor processor() {
    return new StudentProcessor();
  }

  @Bean
  public StaxEventItemReader<Student> reader() {
    StaxEventItemReader<Student> reader = new StaxEventItemReader<>();
    reader.setResource(new ClassPathResource("student.xml"));
    reader.setFragmentRootElementName("student");
    reader.setUnmarshaller(unMarshaller());
    return reader;
  }

  public Unmarshaller unMarshaller() {
    XStreamMarshaller unMarshal = new XStreamMarshaller();
    unMarshal.setAliases(new HashMap<String, Class>() {{
      put("student", Student.class);
    }});
    return unMarshal;
  }

  @Bean
  public FlatFileItemWriter<Student> writer() {
    FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();
    writer.setResource(new FileSystemResource("csv/student.csv"));
    writer.setLineAggregator(new DelimitedLineAggregator<Student>() {{
      setDelimiter(",");
      setFieldExtractor(new BeanWrapperFieldExtractor<Student>() {{
        setNames(new String[]{"rollNo", "name", "department"});
      }});
    }});
    return writer;
  }
}
