<?php

// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/car-carousel.php';

function enqueue_ev_range_ranking_css()
{
    wp_enqueue_style('ev-range_ranking-style', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-range-ranking.css');
}

function ev_range_ranking_shortcode()
{
    enqueue_ev_range_ranking_css();

   $ev_range_data = get_ev_range_ranking_data();
    ob_start();
?>
    <div class="ev-range-ranking-widget">
		<div class="popular-ev-head">
			  <h2 class="wa-title-text">EV Range Ranking</h2>
		</div>
      
        <ul class="ev-range-ranking-list">
            <div class="ev-range-ranking-item">
                <ul>
                    <?php
                    foreach ($ev_range_data as $ev_range) {
                    ?>
                        <div class="ev-range-ranking-item">
                            <div class="ev-range-ranking-thumbnail">
                                <?php
                                if ($ev_range['thumbnail']) {
                                    echo '<img src="' . esc_url($ev_range['thumbnail']) . '" alt="' . esc_attr($ev_range['title']) . '" />';
                                }
                                ?>
                            </div>
                            <div class="ev-range-ranking-right">
                                <div class="ev-range-ranking-title">
                                    <?php echo '<a href="' . get_the_permalink() . '">' . $ev_range['title'] . '</a>'; ?>
                                </div>
                                <div class="ev-range-ranking-range">
                                    <?php
                                    if ($ev_range['min_ev_range'] && $ev_range['max_ev_range']) {
                                        $ev_range = $ev_range['min_ev_range'] . ' - ' . $ev_range['max_ev_range'];
                                        echo $ev_range . ' km';
                                    } else if ($ev_range['max_ev_range']) {
                                        echo $ev_range['max_ev_range'] . ' km';
                                    } else {
                                        echo 'N/A';
                                    }
                                    ?>
                                </div>
                            </div>
                        </div>
                    <?php } ?>
                </ul>
            </div>
        </ul>
    </div>
<?php
    wp_reset_postdata();
    return ob_get_clean();
}
add_shortcode('ev_range_ranking', 'ev_range_ranking_shortcode');
