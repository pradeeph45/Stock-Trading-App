package com.trading.userservice.service;


import com.trading.userservice.dto.AuthResponse;
import com.trading.userservice.dto.LoginRequest;
import com.trading.userservice.dto.RegisterRequest;
import com.trading.userservice.dto.UserResponse;

import java.math.BigDecimal;

public interface UserService {

    public AuthResponse register(RegisterRequest request);

    public AuthResponse login(LoginRequest request);

    public UserResponse getUserById(String userId);

    public UserResponse addFunds(String userId, BigDecimal amount);

    public UserResponse deductFunds(String userId, BigDecimal amount);

    public UserResponse creditFunds(String userId, BigDecimal amount);
}
