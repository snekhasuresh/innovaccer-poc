<?php

function generate_reviews_json_ld($listing_name, $reviews)
{
    $total_rating = 0;
    $review_count = count($reviews);

    $json_ld = array(
        "@context" => "https://schema.org/",
        "@type"    => "Product",
        "name"     => $listing_name,
        "review"   => array()
    );

    foreach ($reviews as $review) {
        $total_rating += $review['total_score'];

        $json_ld['review'][] = array(
            "@type" => "Review",
            "author" => array(
                "@type" => "Person",
                "name"  => $review['user_name']
            ),
            "datePublished" => $review['date'],
            "reviewRating"  => array(
                "@type"        => "Rating",
                "ratingValue"  => $review['total_score'],
                "bestRating"   => "5"
            ),
            "positiveNotes" => $review['pros'],
            "negativeNotes" => $review['cons']
        );
    }

    // Calculate average rating
    if ($review_count > 0) {
        $average_rating = $total_rating / $review_count;

        $json_ld['aggregateRating'] = array(
            "@type"       => "AggregateRating",
            "ratingValue" => round($average_rating, 1),
            "reviewCount" => $review_count,
            "bestRating"  => "5"
        );
    }

    echo '<script type="application/ld+json">' . json_encode($json_ld) . '</script>';
}
?>
