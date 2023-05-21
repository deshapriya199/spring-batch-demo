package com.san4n.demo.batch.processor;

import com.san4n.demo.batch.entity.Product;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class ProductProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product item) throws Exception {
        return item;
    }
}
