<?php
function enqueue_recommended_cars_css()
{
    wp_enqueue_style('recommended-cars-style', get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/recommended-cars.css');
    wp_enqueue_script('recommended-cars-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/js/recommended-cars.js', array(), null, true);
}

function display_cars($cars, $tab_type = 'popular')
{
    enqueue_recommended_cars_css();
    ob_start();

    if (!empty($cars)):
?>
        <ul class="custom-recommended-car-list">
            <?php foreach ($cars as $car): ?>
                <li class="custom-recommended-car-item">
                    <a href="<?php echo $car['permalink']; ?>" class="custom-recommended-car-thumbnail">
                        <?php
                        if ($car['thumbnail_url']): ?>
                            <img src="<?php echo esc_attr($car['thumbnail_url']); ?>" alt="<?php echo esc_attr($car['post_title']); ?>">
                        <?php else: ?>
                            <img src="https://via.placeholder.com/80" alt="No Image Available">
                        <?php endif; ?>
                    </a>
                    <div class="custom-recommended-car-info">
                        <a href="<?php echo $car['permalink']; ?>" class="custom-car-title">
                            <?php
                            $trimmed_title = wp_trim_words($car['post_title'], 2, '...');
                            echo esc_html($trimmed_title);
                            ?>
                        </a>

                        <p class="custom-recommended-car-price">
                            <span class="custom-recommended-price"><?php echo $car['price_range']; ?></span>
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
	$translate = [
		'Recommended car models' => 'Các mẫu xe đề xuất',
		'Popular' => 'Phổ biến',
		'Latest' => 'Mới nhất',
	];

    enqueue_recommended_cars_css();
    // Get popular and latest cars but don't display them immediately
    $popular_cars = get_popular_cars_data();
    $latest_cars = get_latest_cars_data();

    // limit the count of cars to 5
    $popular_cars = array_slice($popular_cars, 0, 5);
    $latest_cars = array_slice($latest_cars, 0, 5);

    ob_start();
    $get_title = get_the_archive_title();
    $clean_title = str_replace('Archives: ', '', $get_title);

    // Remove any unexpected or invisible characters
    $clean_title = preg_replace('/[^\w\s]/u', '', $clean_title);

    $clean_title = trim($clean_title);

    if ($clean_title === 'spanNewsspan') {
        $set_title = 'Popular Cars';
    } else {
        $set_title = $translate['Recommended car models'];
    }

    ?>

    <div class="custom-recommended-cars">

        <h2 class="wa-title-text"><?php echo esc_html($set_title); ?></h2>
        <ul class="custom-recommended-tabs">
            <li class="custom-recommended-tab-link current" data-tab="custom-recommended-tab-1"><?php echo $translate['Popular']; ?></li>
            <li class="custom-recommended-tab-link" data-tab="custom-recommended-tab-2"><?php echo $translate['Latest']; ?></li>
        </ul>

        <div id="custom-recommended-tab-1" class="custom-recommended-tab-content current">
            <!-- Display only popular cars in this tab -->
            <?php echo display_cars($popular_cars, 'เป็นที่นิยม'); ?>
        </div>

        <div id="custom-recommended-tab-2" class="custom-recommended-tab-content">
            <!-- Display only latest cars in this tab -->
            <?php echo display_cars($latest_cars, 'ล่าสุด'); ?>
        </div>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('recommended_cars', 'recommended_cars_shortcode');
