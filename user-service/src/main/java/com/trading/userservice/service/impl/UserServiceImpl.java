package com.trading.userservice.service.impl;

import com.trading.userservice.dto.AuthResponse;
import com.trading.userservice.dto.LoginRequest;
import com.trading.userservice.dto.RegisterRequest;
import com.trading.userservice.dto.UserResponse;
import com.trading.userservice.entity.User;
import com.trading.userservice.repository.UserRepository;
import com.trading.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;

import io.jsonwebtoken.security.Keys;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final String USER_REGISTERED_TOPIC = "user.registered";

    private final long accessTokenExpiration = 15 * 60 * 1000L; // 15 minutes
    private final long refreshTokenExpiration = 7 * 24 * 60 * 60 * 1000L; // 7 days

    @Value("${jwt.secret}")
    private String jwtSecret;

    private SecretKey getSigningKey() {
        return Keys.hmacShaKeyFor(
                Decoders.BASE64.decode(jwtSecret)
        );
    }


    @Override
    public AuthResponse register(RegisterRequest request) {
        log.info("registering user : {}", request.getEmail());

        if(userRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Email already registered "+ request.getEmail());
        }

        User user = new User();
        user.setFirstName(request.getFirstName());
        user.setLastName(request.getLastName());
        user.setEmail(request.getEmail());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setWalletBalance(
                request.getInitialDeposit() != null ? request.getInitialDeposit() : BigDecimal.valueOf(10000)
        );

        User savedUser = userRepository.save(user);

        log.info("User registered : {}" +savedUser.getId());

        //Publish user.registered event
        Map<String,Object> userRegisteredEvent = new HashMap<>();
        userRegisteredEvent.put("userId",savedUser.getId());
        userRegisteredEvent.put("email",savedUser.getEmail());
        userRegisteredEvent.put("firstName",savedUser.getFirstName());
        userRegisteredEvent.put("lastName",savedUser.getLastName());
        userRegisteredEvent.put("walletBalance",savedUser.getWalletBalance());

        kafkaTemplate.send(USER_REGISTERED_TOPIC,savedUser.getId(),userRegisteredEvent);

        String accessToken = generateAccessToken(savedUser.getId(),savedUser.getEmail());

        String refreshToken = generateRefreshToken(savedUser.getId());

        return buildAuthResponse(savedUser,accessToken,refreshToken);
    }

    @Override
    public AuthResponse login(LoginRequest request) {

         log.info("login attempt : {}", request.getEmail());

         User user = userRepository.findByEmail(request.getEmail())
                 .orElseThrow(() -> new RuntimeException(
                         "User not found : "+request.getEmail()
                 ));

         if(!passwordEncoder.matches(request.getPassword(),user.getPassword())){
             throw new RuntimeException("Invalid credentials");
         }

         log.info("Login successful");

        String accessToken = generateAccessToken(user.getId(),user.getEmail());

        String refreshToken = generateRefreshToken(user.getId());

        return buildAuthResponse(user,accessToken,refreshToken);

    }

    /*
     *Get User by ID
     * Called by other services via Feign
     */
    public UserResponse getUserById(String userId){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User not found with id : " + userId
                ));

        return mapToResponse(user);
    }

    /**
     * Adds the specified amount to a user's wallet balance.
     * Retrieves the user, updates the wallet balance, and saves the
     * updated user details to the database.
     *
     * @param userId the ID of the user whose wallet balance will be increased
     * @param amount the amount to be added to the wallet balance
     * @return the updated user details after adding the funds
     * @throws RuntimeException if the user with the specified ID is not found
     */
    public UserResponse addFunds(String userId, BigDecimal amount){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User not found with id : " + userId
                ));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds added : {} to user : {}", amount, userId);

        return mapToResponse(savedUser);
    }

    /**
     * Deducts the specified amount from a user's wallet balance.
     * Checks whether the user exists and has sufficient funds before
     * deducting the amount and saving the updated wallet balance.
     *
     * @param userId the ID of the user whose wallet will be debited
     * @param amount the amount to be deducted from the wallet
     * @return the updated user details after deducting the funds
     * @throws RuntimeException if the user is not found or has insufficient wallet balance
     */
    public UserResponse deductFunds(String userId, BigDecimal amount){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User not found with id : " + userId
                ));

        if(user.getWalletBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient Wallet Balance");
        }

        user.setWalletBalance(user.getWalletBalance().subtract(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds deducted : {} to user : {}", amount, userId);

        return mapToResponse(savedUser);
    }

    /**
     * Credits the specified amount to a user's wallet balance.
     *
     * @param userId the ID of the user whose wallet should be credited
     * @param amount the amount to be credited to the wallet
     * @return the updated user details after crediting the funds
     * @throws RuntimeException if the user with the specified ID is not found
     */
    public UserResponse creditFunds(String userId, BigDecimal amount){
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User not found with id : " + userId
                ));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds credited : {} to user : {}", amount, userId);

        return mapToResponse(savedUser);
    }

    private UserResponse mapToResponse(User user){
        UserResponse response = new UserResponse();
        response.setId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setWalletBalance(user.getWalletBalance());
        response.setStatus(user.getStatus());
        response.setCreatedAt(user.getCreatedAt());

        return response;
    }

    private String generateAccessToken(String userId, String email) {
        Date now = new Date();
        Date expiryDate = new Date(
                now.getTime() + accessTokenExpiration
        );

        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("email", email)
                .claim("type", "access")
                .issuedAt(now)
                .expiration(expiryDate)
                .signWith(getSigningKey())
                .compact();
    }

    private String generateRefreshToken(String userId) {
        Date now = new Date();
        Date expiryDate = new Date(
                now.getTime() + refreshTokenExpiration
        );

        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("type", "refresh")
                .issuedAt(now)
                .expiration(expiryDate)
                .signWith(getSigningKey())
                .compact();
    }

    private AuthResponse buildAuthResponse(
            User user,
            String accessToken,
            String refreshToken) {

        AuthResponse response = new AuthResponse();

        response.setUserId(user.getId());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setEmail(user.getEmail());
        response.setWalletBalance(user.getWalletBalance());
        response.setAccessToken(accessToken);
        response.setRefreshToken(refreshToken);

        return response;
    }
}
