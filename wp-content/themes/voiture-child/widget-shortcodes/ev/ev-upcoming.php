<?php
function enqueue_ev_upcoming_css()
{
    wp_enqueue_style('ev-upcoming-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-upcoming.css');
}

function upcoming_ev_shortcode()
{
    enqueue_ev_upcoming_css();

    $cache_key = 'ev_upcoming';
    $upcoming_car_models_launch_time = get_transient($cache_key);

    if (false === $upcoming_car_models_launch_time) {
        $args = array(
            'post_type'      => 'upcoming-car',
            'posts_per_page' => -1,
        );

        $upcoming_car_models = new WP_Query($args);

        $upcoming_car_models_launch_time = array();
        foreach ($upcoming_car_models->posts as $upcoming_car_model) {
            $launch_time = get_post_meta($upcoming_car_model->ID, 'Time_to_launch', true);
            $weight = get_post_meta($upcoming_car_model->ID, 'weight', true);
            $car_post = get_post($upcoming_car_model->post_parent);
            $is_ev = get_post_meta($upcoming_car_model->post_parent, 'is_ev', true);
            if ($is_ev == 1) {
                $upcoming_car_models_launch_time[] = array(
                    'id'   => $car_post->ID,
                    'title'   => $car_post->post_title,
                    'launch_time' => $launch_time,
                    'weight' => $weight
                );
            }
        }

        // order by launch time
        usort($upcoming_car_models_launch_time, function ($a, $b) {
            return $b['launch_time'] <=> $a['launch_time'];
        });

        // order by weight
        // usort($upcoming_car_models_launch_time, function ($a, $b) {
        //     return $b['weight'] <=> $a['weight'];
        // });

        // take only first upcoming car
        $upcoming_car_models_launch_time = array_slice($upcoming_car_models_launch_time, 0, 1);
        set_transient($cache_key, $upcoming_car_models_launch_time, HOUR_IN_SECONDS);
    }
    ob_start();
?>
    <div class="upcoming-ev-wrapper">
        <h2 class="widget-title wa-title-text"> Upcoming EVs</h2>
        <div class="upcoming-ev-carousel">
            <?php if (count($upcoming_car_models_launch_time) > 0) : ?>
                <?php foreach ($upcoming_car_models_launch_time as $upcoming_car_model) : ?>
                    <?php
                    $ev_car_model_id = $upcoming_car_model['id'];
                    $ev_car_model_title = $upcoming_car_model['title'];

                    $post_thumbnail_id = get_post_thumbnail_id($ev_car_model_id);
                    $thumbnail_post = get_post($post_thumbnail_id);
                    $image_url = $thumbnail_post ? $thumbnail_post->guid : '';
                    ?>
                    <div class="upcoming-ev-item">
                        <div class="upcoming-ev-card">
                            <div class="upcoming-ev-image-container">
                                <img src="<?php echo $image_url; ?>" alt="<?php echo $ev_car_model_title; ?>">
                            </div>
                            <div>
                                <div class="upcoming-ev-name"><?php echo $ev_car_model_title; ?></div>
                                <div class="upcoming-ev-launch-time"><?php echo $upcoming_car_model['launch_time']; ?></div>
                            </div>
                        </div>
                    </div>
                <?php endforeach;
                wp_reset_postdata();
                ?>
            <?php endif; ?>
        </div>

    </div>

<?php
    wp_reset_postdata();
    return ob_get_clean();
}

add_shortcode('upcoming_ev', 'upcoming_ev_shortcode');
