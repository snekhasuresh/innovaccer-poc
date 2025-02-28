<?php
function gallery_car_faqs_shortcode($atts)
{
    $atts = shortcode_atts(array(
        'brand_id' => '',
    ), $atts);

    $brand_id = $atts['brand_id'];

    $args = array(
        'post_type'      => 'faq',
        // 'posts_per_page' => 6,  
        'orderby'        => 'date',
        'order'          => 'DESC',
    );

    if (!empty($brand_id)) {
        $listing_ids = wp_list_pluck(get_posts([
            'post_type'      => 'listing',
            'posts_per_page' => -1,
            'meta_query'     => [['key' => '_listing_make', 'value' => $brand_id, 'compare' => '=']]
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
                                $post_id = get_the_ID(); // Retrieve the current post ID
                                $meta_data = get_post_meta($post_id); // Use the retrieved ID to get post meta
                                // Check if 'question' and 'answer' exist in the meta data
                                $question = isset($meta_data['question'][0]) ? esc_html($meta_data['question'][0]) : 'No question available';
                                $answer = isset($meta_data['answer'][0]) ? wp_kses_post($meta_data['answer'][0]) : 'No answer available';

                                echo '<div class="accordion-item">';
                                echo '<input type="checkbox" id="faq-question-' . $post_id . '">';
                                echo '<div class="accordion-header" id="faq-question-' . $post_id . '">
										 <div class="question">' . $question . '</div>
										 <div class="arrow">›</div>
									</div>';

                                echo '<div class="accordion-content">' . $answer . '</div>';
                                echo '</div>';
                            }
                            wp_reset_postdata();
                            ?>
                        </div>

                    </div>
                </div>
            </div>
        </div>
        <style>
            .gallery {
                margin-left: 0px;
                margin-right: 0px;
            }

            .faq-gallery-container h2 {
                text-align: center;
                margin-bottom: 20px;
            }


            .acc {
                padding-left: 30px;
                padding-right: 30px;
            }

            .accordion {
                max-width: 100%;
                font-family: "Roboto";
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                border-left: 1px solid #e0e0e0;
                border-right: 1px solid #e0e0e0;
                border-top: 1px solid #e0e0e0;
            }

            .accordion-item {
                background: white;
                border-bottom: 1px solid #e0e0e0;
                position: relative;
            }

            .accordion-header {
                padding: 12px 0px;
                cursor: pointer;
                color: #262626;
                position: relative;
                user-select: none;
                font-weight: 700;
                display: block;
                background: white;
                font-size: 16px;
                display: flex;
                gap: 17px;
            }

            .accordion-header {
                display: flex;
                align-items: center;
                justify-content: space-between;
                /* Push question and arrow to opposite sides */
                position: relative;
                cursor: pointer;
            }

            .question {
                flex: 1;
                /* Allows the question to take up available space */
                font-size: 16px;
                font-weight: normal;
            }

            .arrow {
                font-size: 24px;
                font-weight: bold;
                transform: rotate(90deg);
                transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            }

            /* Rotate the arrow when active */
            .accordion-header.active .arrow {
                transform: rotate(270deg);
            }

            .accordion-content {
                background: white;
                overflow: hidden;
                max-height: 0;
                padding: 0 20px;
                color: #666;
                opacity: 0;
            }

            .accordion-item input[type="checkbox"] {
                display: none;
            }

            .accordion-item {
                z-index: 1;
            }

            .accordion-item input[type="checkbox"]:checked {
                z-index: 2;
            }

            .accordion-item input[type="checkbox"]:checked+.accordion-header {
                background: white;
                z-index: 2;
                font-size: 16px;
                color: #262626;
                font-weight: 700;
                margin-bottom: -15px;
            }

            .accordion-item input[type="checkbox"]:checked+.accordion-header::after {
                transform: translateY(-50%) rotate(-90deg);
            }

            .accordion-item input[type="checkbox"]:checked~.accordion-content {
                max-height: 300px;
                opacity: 1;
                padding: 12px 00px;
                z-index: 2;
                font-size: 14px;
                color: #262626;
                font-weight: 400;
            }

            .accordion-content a {
                color: #576b95;
                font-weight: 700;
            }

            .accordion-item input[type="checkbox"]:checked~* {
                position: relative;
                z-index: 2;
            }
        </style>
<?php

    } else {
        echo '<p>' . esc_html__('No FAQs found.', 'voiture') . '</p>';
    }

    $output = ob_get_clean(); // Get the buffered content

    return $output; // Return the content for the shortcode
}
add_shortcode('gallery_car_faqs_shortcode', 'gallery_car_faqs_shortcode');
