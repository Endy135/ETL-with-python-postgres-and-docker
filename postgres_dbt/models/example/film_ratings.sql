--CTE
WITH film_with_ratings AS (
    SELECT 
        film_id,
        title,
        release_date, 
        price, 
        rating, 
        user_rating
        CASE 
            WHEN user_rating >= 4.5 THEN 'Excellent'
            WHEN user_rating >= 4.0 THEN 'Bien'
            WHEN user_rating >= 3.0  THEN 'Moyen'
            ELSE 'Faible'
        END AS rating_category
    FROM {{ref('films')}}
),

films_with_actors AS (
    SELECT 
        f.film_id,
        f.title,
        STRING_AGG(a.actor_name, ',') AS actors
    FROM {{ ref('films') }}  f
    LEFT JOIN {{ ref('film_actors') }} fa ON f.film_id = fa.film_id
    LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)


SELECT 
    fwf.*,
    fwa.actors
FROM film_with_ratings fwf
LEFT JOIN film_actors fwa ON fwa.film_id = fwf.film_id

