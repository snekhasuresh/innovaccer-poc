<?php

if (! defined('ABSPATH')) {
    exit;
}

global $wpdb;
global $post;

$model_id = $post->ID;
$serialized_value = 's:' . strlen((string)$model_id) . ':"' . $model_id . '";';

$args = array(
    'post_type'  => 'faq',
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
    'posts_per_page' => 4,
);

$faq_posts = new WP_Query($args);

// Query to get data from wp_faq table
// $faq_results = $wpdb->get_results( "SELECT * FROM wp_faq where model_id = $post->ID order by weightage desc limit 3", ARRAY_A );

if (! empty($faq_posts)) {
?>
    <div id="listing-detail-description" class="description inner">
        <h3 class="title wa-title-text"><?php esc_html_e('Frequently Asked Questions', 'voiture'); ?></h3>
        <div class="description-inner">
            <div class="description-inner-wrapper">
                <div class="accordion">
                    <div class="acc">
                        <?php
                        while ($faq_posts->have_posts()) {
                            $faq_posts->the_post();
                            $meta_data = get_post_meta($post->ID);
                            echo '<div class="accordion-item">';
                            echo '<input type="checkbox" id="faq-question-' . $post->ID . '">';
                            echo '<label for="faq-question-' . $post->ID . '" class="accordion-header">' . $meta_data['question'][0] . '</label>';
                            echo '<div class="accordion-contentr">' . $meta_data['answer'][0] . '</div>';
                            echo '</div>';
                        }
                        wp_reset_postdata();
                        ?>
                    </div>

                </div>
            </div>
        </div>
        <?php do_action('wp-cardealer-single-listing-description', $post); ?>
    </div>
<?php
} else {
    echo '<p>' . esc_html__('No FAQs found.', 'voiture') . '</p>';
}
?>

<style>
    .faq-container h2 {
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
	.accordion-header::after {
    content: "›";
    position: absolute;
    right: 20px;
    top: 50%;
    transform: translateY(-50%) rotate(90deg);
    transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    font-size: 24px;
    font-weight: bold;
}
	@media screen and (max-width: 768px) {
.accordion-header::after {
    content: "›";
    position: absolute;
    right: -12px !important;
    top: 50%;
    transform: translateY(-50%) rotate(90deg);
    transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    font-size: 24px;
    font-weight: bold;
}
}
</style>