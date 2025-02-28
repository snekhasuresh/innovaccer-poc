<?php

function get_recommended_cars($type = 'popular')

{

    if ($type === 'popular') {

        $top_car_model_data = get_option('top_car_models', []);

        $top_car_model_ids = [];

        if (!empty($top_car_model_data) && is_array($top_car_model_data)) {
            foreach ($top_car_model_data as $type => $data) {
                if (isset($data['car_models']) && is_array($data['car_models'])) {
                    foreach ($data['car_models'] as $car_model) {
                        if (isset($car_model['id'])) {
                            $top_car_model_ids[] = $car_model['id']; // Add car model ID to the array
                        }
                    }
                }
            }
        }
        return get_posts(array(
            'post_type' => 'listing',
            'posts_per_page' => 5,
            'post__in' => $top_car_model_ids,
            'orderby' => 'post__in'
        ));
    } elseif ($type === 'latest') {
        // Query latest cars including upcoming cars
        return get_posts(array(
            'post_type' => 'upcoming-car', // Include both upcoming-car and listing
            'posts_per_page' => 5,
            'orderby' => 'date',
            'order' => 'DESC',
        ));
    }
    return [];
}

function display_cars($cars, $tab_type = 'popular')
{
    ob_start();

    if (!empty($cars)):
?>
        <ul class="recommended-car-list">
            <?php foreach ($cars as $car): ?>
                <li class="recommended-car-item">
                    <a class="recommended-car-thumbnail" href="<?php echo get_permalink($car->ID); ?>">
                        <?php
                        // Get the post thumbnail ID and retrieve the guid (URL)
                        $post_thumbnail_id = get_post_thumbnail_id($car->ID);
                        $thumbnail_post = get_post($post_thumbnail_id);
                        $thumbnail_url = $thumbnail_post ? $thumbnail_post->guid : '';

                        if ($thumbnail_url): ?>
                            <img src="<?php echo esc_url($thumbnail_url); ?>" alt="<?php echo esc_attr(get_the_title($car->ID)); ?>">
                        <?php else: ?>
                            <img src="https://via.placeholder.com/80" alt="No Image Available">
                        <?php endif; ?>
                    </a>
                    <div class="recommended-car-info">
                        <a href="<?php echo get_permalink($car->ID); ?>" class="car-title">
                            <?php echo get_the_title($car->ID); ?>
                        </a>
                        <p class="recommended-car-price">
                            <?php
                            // Get variants and their prices
                            $variants_query = new WP_Query([
                                'post_type' => 'variant',
                                'posts_per_page' => -1,
                                'meta_query' => [
                                    [
                                        'key' => 'model', // Ensure this matches your custom field for the variant
                                        'value' => $car->ID,
                                        'compare' => 'LIKE',
                                    ],
                                ],
                            ]);

                            $min_price = null;
                            $max_price = null;

                            if ($variants_query->have_posts()) {
                                while ($variants_query->have_posts()) {
                                    $variants_query->the_post();
                                    $price = get_post_meta(get_the_ID(), 'retail_price', true);
                                    if ($price) {
                                        $price = (float)$price; // Ensure it's a float for comparison
                                        if (is_null($min_price) || $price < $min_price) {
                                            $min_price = $price;
                                        }
                                        if (is_null($max_price) || $price > $max_price) {
                                            $max_price = $price;
                                        }
                                    }
                                }
                                wp_reset_postdata();
                            }

                            // Prepare the price range text
                            if (!is_null($min_price) && !is_null($max_price)) {
                                if ($min_price === $max_price) {
                                    echo '<span class="recommended-price">RM ' . number_format($min_price) . '</span>';
                                } else {
                                    echo '<span class="recommended-price">RM ' . number_format($min_price) . ' - RM ' . number_format($max_price) . '</span>';
                                }
                            } else {
                                echo '<span class="recommended-price">Price not available</span>';
                            }
                            ?>
                        </p>
                    </div>
                </li>
            <?php endforeach; ?>
        </ul>
    <?php
    else:
        echo '<p>No cars available at the moment.</p>';
    endif;

    return ob_get_clean();
}

function recommended_cars_shortcode()
{
    // Get popular and latest cars but don't display them immediately
    $popular_cars = get_recommended_cars('popular');
    $latest_cars = get_recommended_cars('latest');

    ob_start();
    ?>

    <div class="recommended-cars">
		<?php if (strpos($_SERVER['REQUEST_URI'], 'news') !== false){ ?>
        <h2 class="wa-title-text">รถยอดนิยม</h2>
		<?php}else{ ?>
		<h2 class="wa-title-text">รถแนะนำสำหรับคุณ</h2>
		<?php } ?>
        <ul class="recommended-tabs font-roboto-condensed">
            <li class="recommended-tab-link current" data-tab="recommended-tab-1">Popular</li>
            <li class="recommended-tab-link" data-tab="recommended-tab-2">Latest</li>
        </ul>

        <div id="recommended-tab-1" class="recommended-tab-content current">
            <!-- Display only popular cars in this tab -->
            <?php echo display_cars($popular_cars, 'popular'); ?>
        </div>

        <div id="recommended-tab-2" class="recommended-tab-content">
            <!-- Display only latest cars in this tab -->
            <?php echo display_cars($latest_cars, 'latest'); ?>
        </div>
    </div>

    <!-- <style>
        .recommended-cars h2 {
            font-size: 24px;
        }

        .recommended-tabs {
            list-style: none;
            padding: 0;
            margin: 0;
            display: flex;
            border-bottom: 2px solid #eee;
        }

        .recommended-tabs .recommended-tab-link {
            padding: 10px 15px;
            cursor: pointer;
            border-bottom: 2px solid transparent;
        }

        .recommended-tabs .recommended-tab-link.current {
            color: #32D0C6;
            border-color: #32D0C6;
            font-weight: bold;
        }

        .recommended-tab-content {
            display: none;
            opacity: 0;
            transition: opacity 0.4s ease;
        }

        .recommended-tab-content.current {
            display: block;
            opacity: 1;
        }

        .recommended-car-list {
            list-style: none;
            padding: 0;
            margin: 0;
        }

        .recommended-car-item {
            display: flex;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }

        .recommended-car-thumbnail img {
            width: 80px;
            height: auto;
            margin-right: 15px;
        }

        .recommended-car-info {
            flex: 1;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }

        .car-title {
            font-size: 15px !important;
            font-weight: bold;
            color: #333;
            text-decoration: none;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 1;
            -webkit-box-orient: vertical;
            line-height: 1.4;
        }

        .recommended-car-price {
            font-size: 13px;
            color: #777;
        }
    </style> -->

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            var tabLinks = document.querySelectorAll('.recommended-tab-link');
            var tabContents = document.querySelectorAll('.recommended-tab-content');

            tabLinks.forEach(function(link) {
                link.addEventListener('click', function() {
                    var tabId = this.getAttribute('data-tab');

                    tabLinks.forEach(function(link) {
                        link.classList.remove('current');
                    });

                    tabContents.forEach(function(content) {
                        content.classList.remove('current');
                    });

                    this.classList.add('current');
                    document.getElementById(tabId).classList.add('current');
                });
            });
        });
    </script>

<?php
    return ob_get_clean();
}

add_shortcode('recommended_cars', 'recommended_cars_shortcode');
