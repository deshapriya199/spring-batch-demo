package com.san4n.demo.batch.repository;

import com.san4n.demo.batch.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
