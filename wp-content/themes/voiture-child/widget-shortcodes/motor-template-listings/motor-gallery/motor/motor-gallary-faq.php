<?php
function enqueue_motor_gallery_faq_css()
{
    wp_enqueue_style('gallery-faq-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/motor-gallery/motor/css/gallery-faq.css');
}

function gallery_motor_faqs_shortcode($atts)
{
    enqueue_motor_gallery_faq_css();

    $atts = shortcode_atts(array(
        'brand_id' => '',
    ), $atts);

    $brand_id = $atts['brand_id'];

    $args = array(
        'post_type'      => 'motorcycle-faq',
        'orderby'        => 'date',
        'order'          => 'DESC',
    );

    if (!empty($brand_id)) {
        $listing_ids = wp_list_pluck(get_posts([
            'post_type'      => 'motorcycle-listing',
            'posts_per_page' => -1,
            'meta_query'     => [['key' => 'make', 'value' => $brand_id, 'compare' => '=']]
        ]), 'ID');

        if (!empty($listing_ids)) {
            $meta_query = ['relation' => 'OR'];

            foreach ($listing_ids as $listing_id) {
                $meta_query[] = [
                    'key'     => 'related_models',
                    'value'   => sprintf(':"%d";', $listing_id),
                    'compare' => 'LIKE'
                ];
            }

            $args['meta_query'] = $meta_query;
        }
    }

    $faq_posts = new WP_Query($args);

    ob_start();
    if ($faq_posts->have_posts()) {
?>
        <h2 class="wa-title-text">FAQ</h2>
        <div id="listing-detail-gallery" class="gallery inner">
            <div class="description-inner">
                <div class="description-inner-wrapper">
                    <div class="accordion">
                        <div class="acc">
                            <?php
                            while ($faq_posts->have_posts()) {
                                $faq_posts->the_post();
                                $post_id = get_the_ID();
                                $meta_data = get_post_meta($post_id);

                                $question = isset($meta_data['question'][0]) ? esc_html($meta_data['question'][0]) : 'No question available';
                                $answer = isset($meta_data['answer'][0]) ? wp_kses_post($meta_data['answer'][0]) : 'No answer available';
                            ?>
                                <div class="accordion-item">
                                    <input type="checkbox" id="faq-question-<?php echo $post_id; ?>" class="accordion-toggle">
                                    <label for="faq-question-<?php echo $post_id; ?>" class="accordion-header">
                                        <div class="question"><?php echo $question; ?></div>
                                        <div class="arrow">›</div>
                                    </label>
                                    <div class="accordion-content"><?php echo $answer; ?></div>
                                </div>
                            <?php
                            }
                            wp_reset_postdata();
                            ?>
                        </div>
                    </div>
                </div>
            </div>
        </div>
<?php
    } else {
        echo '<p>' . esc_html__('No FAQs found.', 'voiture') . '</p>';
    }

    return ob_get_clean();
}
add_shortcode('gallery_motor_faqs_shortcode', 'gallery_motor_faqs_shortcode');
