package com.dk0124.migration;

import org.modelmapper.ModelMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import com.dk0124.cdr.persistence.repositoryUtils.bithumb.candle.BithumbCandleRepositoryUtils;

@Configuration
@ComponentScan(basePackages = {"com.dk0124.cdr"})
@EnableJpaRepositories(basePackages = {"com.dk0124.cdr"})
@EntityScan(basePackages = {"com.dk0124.cdr"})
public class Config {
	@Bean
	@ConditionalOnMissingBean(ModelMapper.class)
	public ModelMapper modelMapper() {
		return new ModelMapper();
	}
}
