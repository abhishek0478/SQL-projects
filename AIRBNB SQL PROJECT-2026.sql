DROP TABLE IF EXISTS listings;

CREATE TABLE listings (
    listing_id BIGINT,
    host_id BIGINT,
    host_since DATE,
    host_response_time TEXT,
    host_response_rate TEXT,
    host_acceptance_rate TEXT,
    host_is_superhost BOOLEAN,
    host_total_listings_count INT,
    host_has_profile_pic BOOLEAN,
    host_identity_verified BOOLEAN,
    neighbourhood TEXT,
    district TEXT,
    city TEXT,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    property_type TEXT,
    room_type TEXT,
    accommodates INT,
    bedrooms INT,
    price NUMERIC,
    minimum_nights INT,
    maximum_nights INT,
    review_scores_rating NUMERIC,
    review_scores_accuracy NUMERIC,
    review_scores_cleanliness NUMERIC,
    review_scores_checkin NUMERIC,
    review_scores_communication NUMERIC,
    review_scores_location NUMERIC,
    review_scores_value NUMERIC,
    instant_bookable BOOLEAN
);

CREATE TABLE reviews (
    listing_id BIGINT,
    review_id BIGINT,
    date DATE,
    reviewer_id BIGINT
);

Select * from reviews
limit(10);

--Intermediate SQL Questions

--1.Find the top 10 most expensive listings in each city.
SELECT *
FROM (
    SELECT 
        listing_id,
        city,
        price,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY price DESC) AS rank_in_city
    FROM listings
) t
WHERE rank_in_city <= 10
ORDER BY city, rank_in_city;


--Calculate the average price of listings by property_type.

SELECT 
    property_type,
    Round(AVG(price),2) as average_price
FROM listings
GROUP BY property_type
ORDER BY average_price DESC;

--Find the number of listings available in each neighbourhood.
SELECT 
    neighbourhood,
    COUNT(listing_id) AS total_listings
FROM listings
GROUP BY neighbourhood
ORDER BY total_listings DESC;

--List the hosts who own more than 5 listings.
SELECT 
    host_id,
    COUNT(listing_id) AS total_listings
FROM listings
GROUP BY host_id
HAVING COUNT(listing_id) > 5
ORDER BY total_listings DESC;

--Find the average review_scores_rating for each city.
SELECT 
    city,
    ROUND(AVG(review_scores_rating), 2) AS avg_rating
FROM listings
GROUP BY city
ORDER BY avg_rating DESC;

or

SELECT 
    city,
    ROUND(AVG(review_scores_rating), 2) AS avg_rating,
    COUNT(review_scores_rating) AS rated_listings
FROM listings
WHERE review_scores_rating IS NOT NULL
GROUP BY city
ORDER BY avg_rating DESC;

--Find the number of reviews for each listing.
SELECT 
    listing_id,
    COUNT(review_id) AS total_reviews
FROM reviews
GROUP BY listing_id
ORDER BY total_reviews DESC;

--Find the top 5 neighbourhoods with the highest average price.
SELECT 
    neighbourhood,
    AVG(price) AS avg_price
FROM listings
GROUP BY neighbourhood
ORDER BY avg_price DESC
LIMIT 5;

--Count how many listings are instant bookable vs not instant bookable.
SELECT 
    instant_bookable,
    COUNT(*) AS total_listings
FROM listings
GROUP BY instant_bookable
ORDER BY instant_bookable;

--Find the average accommodates capacity per room_type.
SELECT 
    room_type,
    ROUND(AVG(accommodates), 2) AS avg_accommodates
FROM listings
GROUP BY room_type
ORDER BY avg_accommodates DESC;

--Find listings where price is higher than the average price in that city.
SELECT 
    listing_id,
    city,
    price,
    city_avg_price
FROM (
        SELECT 
            listing_id,
            city,
            price,
            AVG(price) OVER (PARTITION BY city) AS city_avg_price
        FROM listings
     ) t
WHERE price > city_avg_price
ORDER BY city, price DESC;


--Find the top 3 most reviewed listings in each city using window functions.


WITH review_count AS (
    SELECT 
        l.listing_id,
        l.city,
        COUNT(r.review_id) AS total_reviews
    FROM listings l
    LEFT JOIN reviews r
        ON l.listing_id = r.listing_id
    GROUP BY l.listing_id, l.city
),
ranked_listings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_reviews DESC) AS rank_in_city
    FROM review_count
)
SELECT 
    listing_id,
    city,
    total_reviews
FROM ranked_listings
WHERE rank_in_city <= 3
ORDER BY city, rank_in_city;


--Find the hosts with the highest average review rating (minimum 10 reviews).
SELECT 
    l.host_id,
    ROUND(AVG(l.review_scores_rating), 2) AS avg_rating,
    COUNT(r.review_id) AS total_reviews
FROM listings l
JOIN reviews r
    ON l.listing_id = r.listing_id
WHERE l.review_scores_rating IS NOT NULL
GROUP BY l.host_id
HAVING COUNT(r.review_id) >= 10
ORDER BY avg_rating DESC;


--Calculate the monthly number of reviews for each listing.
SELECT 
    listing_id,
    TO_CHAR(date, 'YYYY-MM') AS review_month,
    COUNT(*) AS total_reviews
FROM reviews
GROUP BY listing_id, review_month
ORDER BY listing_id, review_month;

--Find the price difference between a listing and the average price in its neighbourhood.
SELECT 
    listing_id,
    neighbourhood,
    price,
    ROUND(AVG(price) OVER (PARTITION BY neighbourhood), 2) AS neighbourhood_avg_price,
    ROUND(price - AVG(price) OVER (PARTITION BY neighbourhood), 2) AS price_difference
FROM listings
ORDER BY neighbourhood, price_difference DESC;


--Identify superhosts whose listings have ratings below the city average.

WITH city_rating AS (
    SELECT 
        listing_id,
        host_id,
        city,
        review_scores_rating,
        host_is_superhost,
        AVG(review_scores_rating) OVER (PARTITION BY city) AS city_avg_rating
    FROM listings
    WHERE review_scores_rating IS NOT NULL
)
SELECT 
    listing_id,
    host_id,
    city,
    review_scores_rating,
    ROUND(city_avg_rating,2) AS city_avg_rating
FROM city_rating
WHERE host_is_superhost = TRUE
AND review_scores_rating < city_avg_rating
ORDER BY city, review_scores_rating;

--Find the top 5 hosts generating the highest potential revenue, calculated as:
--price × minimum_nights
WITH host_revenue AS (
    SELECT 
        host_id,
        SUM(price * minimum_nights) AS potential_revenue
    FROM listings
    GROUP BY host_id
)
SELECT *,
       RANK() OVER (ORDER BY potential_revenue DESC) AS revenue_rank
FROM host_revenue
WHERE potential_revenue IS NOT NULL;

--Rank listings within each city based on review_scores_rating.
SELECT 
    listing_id,
    city,
    review_scores_rating,
    RANK() OVER (
        PARTITION BY city 
        ORDER BY review_scores_rating DESC
    ) AS rating_rank
FROM listings
WHERE review_scores_rating IS NOT NULL
ORDER BY city, rating_rank;

--Find listings that have reviews from more than 50 unique reviewers.
SELECT 
    l.listing_id,
    l.city,
    COUNT(DISTINCT r.reviewer_id) AS unique_reviewers
FROM listings l
JOIN reviews r
    ON l.listing_id = r.listing_id
GROUP BY l.listing_id, l.city
HAVING COUNT(DISTINCT r.reviewer_id) > 50
ORDER BY unique_reviewers DESC;

--Find the percentage of listings that are instant_bookable in each city.
SELECT 
    city,
    ROUND(
        100.0 * SUM(CASE WHEN instant_bookable = TRUE THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS instant_bookable_percentage
FROM listings
GROUP BY city
ORDER BY instant_bookable_percentage DESC;

--Identify listings where the review rating improved over time (compare earliest review vs latest review).
WITH monthly_reviews AS (
    SELECT 
        listing_id,
        DATE_TRUNC('month', date) AS month,
        COUNT(*) AS reviews_count
    FROM reviews
    GROUP BY listing_id, month
),
trend AS (
    SELECT *,
           LAG(reviews_count) OVER (
               PARTITION BY listing_id
               ORDER BY month
           ) AS prev_month_reviews
    FROM monthly_reviews
)
SELECT *
FROM trend
WHERE reviews_count > prev_month_reviews;




















