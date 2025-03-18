<?php

// import css from faq.css
function faq_css()
{
    wp_enqueue_style('faq', get_stylesheet_directory_uri() . '/widget-shortcodes/individual-listing/css/faq.css');
}

add_shortcode('individual_listing_faq', 'individual_listing_faq_shortcode');
function individual_listing_faq_shortcode($atts)
{
    faq_css();

    $listing_post = get_listing_from_query_vars()['post'];

    $serialized_value = 's:' . strlen((string) $listing_post->ID) . ':"' . $listing_post->ID . '";';

    // get all FAQs for the listing
    $args = array(
        'post_type' => 'faq',
        'posts_per_page' => 5,
        'meta_query' => array(
            array(
                'key'     => 'related_models',
                'value'   => $serialized_value,
                'compare' => 'LIKE',
            ),
        ),
        'meta_key'   => 'weight',
        'orderby'    => 'meta_value',
        'order'      => 'DESC',
    );
    $faqs = new WP_Query($args);

    if ($faqs->have_posts()) {
        $json_ld_faqs = [];
        while ($faqs->have_posts()) {
            $faqs->the_post();
            $post_id = get_the_ID();
            $meta_data = get_post_meta($post_id);
            $question = isset($meta_data['question'][0]) ? esc_html($meta_data['question'][0]) : 'No question available';
            $answer = isset($meta_data['answer'][0]) ? wp_kses_post($meta_data['answer'][0]) : 'No answer available';

            $json_ld_faqs[] = array(
                'id' => $post_id,
                'question' => $question,
                'answer' => $answer,
            );
        }
        wp_reset_postdata();

        add_faq_json_ld($json_ld_faqs);
    }

    // if no FAQs found, return
    if (!$faqs->have_posts()) {
        wp_reset_postdata();
        return ''; // Return empty if no FAQs are found
    }

    ob_start();
    display_faq($faqs->posts, $listing_post);
    return ob_get_clean();
}

function display_faq($faq_posts, $listing_post)
{
    if (! empty($faq_posts)) {
?>
        <h2 class="title wa-title-text">Câu hỏi thường gặp về  <?php echo $listing_post->post_title; ?></h2>
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">
                        <?php
                        foreach ($faq_posts as $post) {
                            $meta_data = get_post_meta($post->ID);
                            echo '<div class="accordion-item">';
                            echo '<input type="checkbox" id="faq-question-' . $post->ID . '">';
                            echo '<label for="faq-question-' . $post->ID . '" class="accordion-header">' . $meta_data['question'][0] . '</label>';
                            echo '<div class="accordion-content">' . $meta_data['answer'][0] . '</div>';
                            echo '</div>';
                        }
                        wp_reset_postdata();
                        ?>
                    </div>

                </div>
            </div>
        </div>
        <?php do_action('wp-cardealer-single-listing-description', $post); ?>
<?php
    } else {
        echo '<h3>' . $listing_post->post_title . 'FAQs</h3>';
        echo '<p>' . esc_html__(' ', 'voiture') . '</p>';
    }
}
