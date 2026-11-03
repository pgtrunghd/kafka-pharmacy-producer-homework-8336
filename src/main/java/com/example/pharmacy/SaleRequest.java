package com.example.pharmacy;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
public record SaleRequest(@NotBlank String medicineId, @Positive int quantity) {}
